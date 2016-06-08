package org.jah.nifi.geo

import java.io.InputStream
import java.util

import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.{CapabilityDescription, Tags}
import org.apache.nifi.annotation.lifecycle.{OnScheduled, OnStopped}
import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult}
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor._
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.util.StandardValidators
import org.geotools.data.{DataStore, DataStoreFinder, FeatureWriter, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.jah.nifi.geo.PutGeoMesa._
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter
import org.locationtech.geomesa.convert
import org.locationtech.geomesa.convert.{ConverterConfigLoader, ConverterConfigResolver, SimpleFeatureConverters}
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.utils.geotools.{SftArgResolver, SimpleFeatureTypeLoader}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@Tags(Array("geomesa", "geo", "ingest", "convert", "accumulo", "geotools"))
@CapabilityDescription("Convert and ingest data files into GeoMesa")
@InputRequirement(Requirement.INPUT_REQUIRED)
class PutGeoMesa extends AbstractProcessor {

  type SFW = FeatureWriter[SimpleFeatureType, SimpleFeature]

  private var descriptors: java.util.List[PropertyDescriptor] = null
  private var relationships: java.util.Set[Relationship] = null

  protected override def init(context: ProcessorInitializationContext): Unit = {
    descriptors = List(
      IngestModeProp,
      GeoMesaConfigController,
      Zookeepers,
      InstanceName,
      User,
      Password,
      Catalog,
      SftName,
      ConverterName,
      FeatureNameOverride,
      SftSpec,
      ConverterSpec
     ).asJava

    relationships = Set(SuccessRelationship, FailureRelationship).asJava

    getLogger.info(s"Props are ${descriptors.mkString(", ")}")
    getLogger.info(s"Relationships are ${relationships.mkString(", ")}")
  }

  override def getRelationships = {
    getLogger.info(s"getRelationships are ${relationships.mkString(", ")}")
    relationships
  }
  override def getSupportedPropertyDescriptors = {
    getLogger.info(s"getSupportedPropertyDescriptors are ${descriptors.mkString(", ")}")
    descriptors
  }

  @volatile
  protected var dataStore: DataStore = null

  @volatile
  private var featureWriter: SFW = null

  @volatile
  private var converter: convert.SimpleFeatureConverter[_] = null

  @volatile
  private var mode: String = null

  /**
    * Flag to be set in validation
    */
  @volatile
  protected var useControllerService: Boolean = false

  protected def getGeomesaControllerService(context: ProcessContext): GeomesaConfigService = {
    context.getProperty(GeoMesaConfigController).asControllerService().asInstanceOf[GeomesaConfigService]
  }

  protected def getDataStoreFromParams(context: ProcessContext): DataStore =
    DataStoreFinder.getDataStore(Map(
      "zookeepers" -> context.getProperty(Zookeepers).getValue,
      "instanceId" -> context.getProperty(InstanceName).getValue,
      "tableName"  -> context.getProperty(Catalog).getValue,
      "user"       -> context.getProperty(User).getValue,
      "password"   -> context.getProperty(Password).getValue
    ))

  @OnScheduled
  def initialize(context: ProcessContext): Unit = {
    if (useControllerService) {
      dataStore = getGeomesaControllerService(context).getDataStore
    } else {
      dataStore = getDataStoreFromParams(context)
    }

    val sft = getSft(context)
    dataStore.createSchema(sft)
    featureWriter = createFeatureWriter(sft, context)

    mode = context.getProperty(IngestModeProp).getValue
    if (mode == IngestMode.Converter) {
      converter = getConverter(sft, context)
    }

    getLogger.info(s"Initialized GeoMesaIngest datastore, fw, converter for type ${sft.getTypeName}")
  }

  @OnStopped
  def cleanup(): Unit = {
    if (featureWriter != null) {
      IOUtils.closeQuietly(featureWriter)
      featureWriter = null
    }

    if (dataStore != null) {
      dataStore.dispose()
      dataStore = null
    }

    getLogger.info("Shut down GeoMesaIngest processor " + getIdentifier)
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit =
    Option(session.get()).foreach { f =>
      try {
        getLogger.info(s"Processing file ${fullName(f)}")
        doWork(context, session, f)
        featureWriter.asInstanceOf[AccumuloFeatureWriter].flush()
        getLogger.debug("Flushed AccumuloFeatureWriter")
        session.transfer(f, SuccessRelationship)
    } catch {
      case e: Exception =>
        getLogger.error(s"Error: ${e.getMessage}", e)
        session.transfer(f, FailureRelationship)
      }
    }

  type ProcessFn = (ProcessContext, ProcessSession, FlowFile) => Unit
  private def doWork(context: ProcessContext, session: ProcessSession, flowFile: FlowFile): Unit = {
    val fn: ProcessFn = mode match {
      case IngestMode.Converter    => converterIngest()
      case IngestMode.AvroDataFile => avroIngest()
      case o: String =>
        throw new IllegalStateException(s"Unknown ingest type: $o")
    }
    fn(context, session, flowFile)
  }

  def fullName(f: FlowFile) = f.getAttribute("path") + f.getAttribute("filename")

  protected def converterIngest(): ProcessFn =
    (context: ProcessContext, session: ProcessSession, flowFile: FlowFile) => {
      getLogger.debug("Running converter based ingest")
      val fullFlowFileName = fullName(flowFile)
      val ec = converter.createEvaluationContext(Map("inputFilePath" -> fullFlowFileName))
      session.read(flowFile, new InputStreamCallback {
        override def process(in: InputStream): Unit = {
          getLogger.info(s"Converting path $fullFlowFileName")
          converter
            .process(in, ec)
            .foreach { sf =>
              val toWrite = featureWriter.next()
              toWrite.setAttributes(sf.getAttributes)
              toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
              toWrite.getUserData.putAll(sf.getUserData)
              featureWriter.write()
            }
        }
      })
      getLogger.debug(s"Converted and ingested file $fullFlowFileName with ${ec.counter.getSuccess} successes and " +
        s"${ec.counter.getFailure} failures")
    }

  protected def avroIngest(): ProcessFn = (context: ProcessContext, session: ProcessSession, flowFile: FlowFile) => {
    val fullFlowFileName = fullName(flowFile)
    session.read(flowFile, new InputStreamCallback {
      override def process(in: InputStream): Unit = {
        val reader = new AvroDataFileReader(in)
        try {
          dataStore.createSchema(reader.getSft)
          val featureWriter = dataStore.getFeatureWriterAppend(reader.getSft.getTypeName, Transaction.AUTO_COMMIT)
          try {
            reader.foreach { sf =>
              val toWrite = featureWriter.next()
              toWrite.setAttributes(sf.getAttributes)
              toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
              toWrite.getUserData.putAll(sf.getUserData)
              featureWriter.write()
            }
          } finally {
            featureWriter.close()
          }
        } finally {
          reader.close()
        }
      }
    })
    getLogger.debug(s"Ingested avro file $fullFlowFileName")
  }

  private def getSft(context: ProcessContext): SimpleFeatureType = {
    val sftArg = Option(context.getProperty(SftName).getValue)
      .orElse(Option(context.getProperty(SftSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${SftName.getName} or ${SftSpec.getName} property"))
    val typeName = context.getProperty(FeatureNameOverride).getValue
    SftArgResolver.getSft(sftArg, typeName).getOrElse(throw new IllegalArgumentException(s"Could not resolve sft from config value $sftArg and typename $typeName"))
  }

  private def createFeatureWriter(sft: SimpleFeatureType, context: ProcessContext): SFW = {
    dataStore.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)
  }

  private def getConverter(sft: SimpleFeatureType, context: ProcessContext): convert.SimpleFeatureConverter[_] = {
    val convertArg = Option(context.getProperty(ConverterName).getValue)
      .orElse(Option(context.getProperty(ConverterSpec).getValue))
      .getOrElse(throw new IllegalArgumentException(s"Must provide either ${ConverterName.getName} or ${ConverterSpec.getName} property"))
    val config = ConverterConfigResolver.getConfig(convertArg).get
    SimpleFeatureConverters.build(sft, config)
  }

  override def customValidate(validationContext: ValidationContext): java.util.Collection[ValidationResult] = {

    val validationFailures = new util.ArrayList[ValidationResult]()

    useControllerService = validationContext.getProperty(GeoMesaConfigController).isSet
    val paramsSet = Seq(Zookeepers, InstanceName, User, Password, Catalog).forall(validationContext.getProperty(_).isSet)

    // require either controller-service or all of {zoo,instance,user,pw,catalog}
    if (!useControllerService && !paramsSet)
      validationFailures.add(new ValidationResult.Builder()
        .input("Use either GeoMesa Configuration Service, or specify accumulo connection parameters.")
        .build)

    // If using converters checkf or params relevant to that
    def useConverter = validationContext.getProperty(IngestModeProp).getValue == IngestMode.Converter
    if (useConverter) {
      // make sure either a sft is named or written
      val sftNameSet = validationContext.getProperty(SftName).isSet
      val sftSpecSet = validationContext.getProperty(SftSpec).isSet
      if (!sftNameSet && !sftSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a simple feature type by name or spec")
          .build)

      val convNameSet = validationContext.getProperty(ConverterName).isSet
      val convSpecSet = validationContext.getProperty(ConverterSpec).isSet
      if (!convNameSet && !convSpecSet)
        validationFailures.add(new ValidationResult.Builder()
          .input("Specify a converter by name or spec")
          .build
        )
    }

    validationFailures
  }

}

object PutGeoMesa {

  object IngestMode {
    val Converter    = "Converter"
    val AvroDataFile = "AvroDataFile"
  }

  final val SuccessRelationship = new Relationship.Builder().name("success").description("Success").build
  final val FailureRelationship = new Relationship.Builder().name("failure").description("Failure").build

  val GeoMesaConfigController = new PropertyDescriptor.Builder()
    .name("GeoMesa Configuration Service")
    .description("The controller service used to connect to Accumulo")
    .required(false)
    .identifiesControllerService(classOf[GeomesaConfigService])
    .build

  val Zookeepers = new PropertyDescriptor.Builder()
    .name("Zookeepers")
    .description("Zookeepers host(:port) pairs, comma separated")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val InstanceName = new PropertyDescriptor.Builder()
    .name("Instance")
    .description("Accumulo instance name")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val User = new PropertyDescriptor.Builder()
    .name("User")
    .description("Accumulo user name")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val Password = new PropertyDescriptor.Builder()
    .name("Password")
    .description("Accumulo password")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .sensitive(true)
    .build

  val Catalog = new PropertyDescriptor.Builder()
    .name("Catalog")
    .description("GeoMesa catalog table name")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val SftName = new PropertyDescriptor.Builder()
    .name("SftName")
    .description("Choose a simple feature type defined by a GeoMesa SFT Provider (preferred)")
    .required(false)
    .allowableValues(SimpleFeatureTypeLoader.listTypeNames.toArray: _*)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val ConverterName = new PropertyDescriptor.Builder()
    .name("ConverterName")
    .description("Choose an SimpleFeature Converter defined by a GeoMesa SFT Provider (preferred)")
    .required(false)
    .allowableValues(ConverterConfigLoader.listConverterNames.toArray: _*)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  val FeatureNameOverride = new PropertyDescriptor.Builder()
    .name("FeatureNameOverride")
    .description("Override the Simple Feature Type name from the SFT Spec")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build

  // TODO create a custom validator
  val SftSpec = new PropertyDescriptor.Builder()
    .name("SftSpec")
    .description("Manually define a SimpleFeatureType (SFT) config spec")
    .required(false)
    .build

  // TODO create a custom validator
  val ConverterSpec = new PropertyDescriptor.Builder()
    .name("ConverterSpec")
    .description("Manually define a converter using typesafe config")
    .required(false)
    .build

  val IngestModeProp = new PropertyDescriptor.Builder()
    .name("Mode")
    .description("Ingest mode")
    .required(true)
    .allowableValues(Array[String](IngestMode.Converter, IngestMode.AvroDataFile): _*)
    .defaultValue(IngestMode.Converter)
    .build

}