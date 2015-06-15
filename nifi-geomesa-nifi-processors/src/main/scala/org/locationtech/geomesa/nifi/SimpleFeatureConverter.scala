package org.locationtech.geomesa.nifi

import java.io.{InputStream, OutputStream}

import com.typesafe.config.ConfigFactory
import org.apache.avro.file.DataFileWriter
import org.apache.nifi.annotation.behavior.WritesAttribute
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.features.avro.{AvroSimpleFeatureUtils, AvroSimpleFeatureWriter}
import org.locationtech.geomesa.nifi.SimpleFeatureConverter._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.io.Source

@Tags(Array("OGC", "geo", "convert", "converter", "simple feature", "geotools"))
@CapabilityDescription("Convert incoming files into OGC SimpleFeature format")
@WritesAttribute(attribute = "mime.type", description = "the mime type of the outgoing format")
class SimpleFeatureConverter extends AbstractProcessor {

    private var descriptors: java.util.List[PropertyDescriptor] = null
    private var relationships: java.util.Set[Relationship] = null

    protected override def init(context: ProcessorInitializationContext): Unit = {
      descriptors = List(ConverterConfig, SftConfig).asJava
      relationships = Set(SuccessRelationship, FailureRelationship).asJava
    }

    override def getRelationships = relationships
    override def getSupportedPropertyDescriptors = descriptors

    override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
      Option(session.get()).map(doWork(context, session, _))

    private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
      val sft = getSft(context)
      val converter = getConverter(sft, context)
      try {
        val schema = AvroSimpleFeatureUtils.generateSchema(sft)
        val newFlowFile = session.write(flowFile, new StreamCallback {
          override def process(in: InputStream, out: OutputStream): Unit = {
            val dfw = new DataFileWriter[SimpleFeature](new AvroSimpleFeatureWriter(sft))
            dfw.create(schema, out)
            converter.processInput(
              Source.fromInputStream(in)
                .getLines()
                .toList
                .iterator
                .filterNot(s => "^\\s*$".r.findFirstIn(s).size > 0)  // csv converter requires this currently
            ).foreach(dfw.append)
            dfw.close()
          }
        })
        session.transfer(newFlowFile, SuccessRelationship)
      } catch {
        case e: Exception =>
          getLogger.error("oops", e)
          session.transfer(flowFile, FailureRelationship)
      } finally {
        converter.close()
      }
    }

  private def getConverter(sft: SimpleFeatureType, context: ProcessContext) =
    SimpleFeatureConverters.build[String](sft, ConfigFactory.parseString(context.getProperty(ConverterConfig).getValue))

  private def getSft(context: ProcessContext) =
    SimpleFeatureTypes.createType(ConfigFactory.parseString(context.getProperty(SftConfig).getValue))
}

object SimpleFeatureConverter {

  val SftConfig = new PropertyDescriptor.Builder()
    .name("SftConfig")
    .description("SimpleFeatureType (SFT) config")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ConverterConfig = new PropertyDescriptor.Builder()
    .name("ConverterConfig")
    .description("Converter Config")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val FileFormat = new PropertyDescriptor.Builder()
    .name("FileFormat")
    .description("File format for the outgoing simple feature file")
    .required(true)
    .allowableValues(Set("avro").asJava)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build
}