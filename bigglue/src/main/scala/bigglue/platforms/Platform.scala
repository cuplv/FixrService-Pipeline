package bigglue.platforms

import bigglue.configurations.{ConfOpt, Constant, PipeConfig, PlatformBuilder}
import bigglue.connectors.Connector
import bigglue.data.{Identifiable, Identity}
import bigglue.store.DataStore
import com.typesafe.config.Config
import bigglue.checkers.{BinaryChecker, UnaryChecker}
import bigglue.computations.Computation
import bigglue.curators._
import bigglue.exceptions.NotInitializedException
import org.apache.kafka.streams.kstream.Reducer
import java.security.MessageDigest
import java.util.Base64

/**
  * This is the place where we pass the data in through the computation engine.
  * The actual computation engine used is defined in the subclasses of the platform.
  * It has two connectors, upstream and downstream, which influence how the code ends up working.
  * This is created as you call pipe.init() in the example code and can be found in a MapperPipe.mapper.platform,
  * or in ReducerPipe.reducer.platform.
  * More specifically, this is the abstraction above UnaryPlatforms and BinaryPlatforms. In the example code,
  * only UnaryPlatforms have been used.
  */


abstract class Platform {
  /**
  * What computation this platform ends up running. (Mapper, Reducer, PairwiseComposer)
  * This ends up being set in init() by both UnaryPlatform and BinaryPlatform.
  */
  var computationOpt: Option[Computation] = None

  /**
    * This ends up being called in the init step of the Mapper, Reducer, or PairwiseComposer
    * This links the actual computation and platform together.
    * @param computation The computation that is linked with the platform.
    *                    (This is usually called through platform.setComputation(this))
    */
  def setComputation(computation: Computation): Platform = {
    computationOpt = Some(computation)
    this
  }

  def trueVersion(string: String): String = computationOpt match{
    case Some(comp) =>
      val toAdd = comp.versionOpt match{
        case None => "<None>"
        case Some(x) => s"[$x]"
      }
      // val encStr = Base64.getDecoder.decode(string)
      // new String(Base64.getEncoder.encode(MessageDigest.getInstance("SHA-256").digest(s"$encStr|$toAdd".getBytes)))
      s"$string|$toAdd"
    case None => throw new Exception("Expected a Computation")
  }

  def terminate(): Unit

  def run(): Unit = computationOpt match {
    case Some(computation) => computation.run()
    case None => {
      throw new NotInitializedException("Platform", "run()", None)
    }
  }

  /**
    * This is called by the computation's persist step.
    * In particular, this tends to see how much of the data set actually needs to be re-sent.
    */
  def persist(): Unit

  /**
    * This function is called when the upstream connector lets the platform know that there's data to be computed.
    */
  def wake(): Unit

}

/**
  * This is a type of platform that only deals with one input. These have a computation of either Mapper or Reducer.
  * Within the example, we use the default of [[bigglue.platforms.instances.bigactors.BigActorMapperPlatform]] and
  * [[bigglue.platforms.instances.bigactors.BigActorReducerPlatform]].
  * @tparam Input The type of the data that's being sent in.
  * @tparam Output The type of the data that's being sent out.
  */
abstract class UnaryPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]] extends Platform with UnaryChecker[Input,Output] {

  var upstreamConnectorOpt: Option[Connector[Input]] = None
  var provenanceCuratorOpt: Option[ProvenanceCurator[Input,Output]] = None
  var errorCuratorOpt: Option[ErrorCurator[Input]] = None
  var versionCuratorOpt: Option[VersionCurator[Output]] = None
  var inputMapOpt: Option[DataStore[Input]]   = None
  var outputMapOpt: Option[DataStore[Output]] = None

  /**
    * This sets up the platform by connecting the Input Map and Output Map to the platform, as well as
    * any other initialization that needs to be done.
    * In the case of the example, we use [[bigglue.platforms.instances.bigactors.BigActorUnaryPlatform.init]]
    * This calls [[initConnector]] as well.
    * @param conf The configuration file needed to initialize.
    * @param inputMap The Map that data gets sent in from.
    * @param outputMap The Map that data gets sent out to.
    * @param builder The builder that created the platform. This was called with [[bigglue.computations.Mapper.init]] or [[bigglue.computations.Reducer.init]]
    */
  def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    inputMapOpt = Some(inputMap)
    outputMapOpt = Some(outputMap)
    val thisVer = trueVersion(inputMap.sha)
    outputMap.setSha(thisVer)
    computationOpt match{
      case Some(comp) => comp.shaVersionOpt = Some(thisVer)
      case None => throw new Exception("Expected a Computation")
    }
    getVersionCurator()
    initConnector(conf, builder)
    provenanceCuratorOpt = Some(builder.provenanceCurator)
    errorCuratorOpt = Some(builder.errorCurator)
    println("INIT")
    println(getInputMap().name, getOutputMap().name, getVersionCurator().thisVersion)
    println(getInputMap().all().length, getOutputMap().all().length)
  }

  /**
    * This sets up the connector that sends data down the pipeline.
    * This initializes the connector with [[Connector.init]], and then adds
    * the platform to the connector with [[Connector.registerPlatform]].
    * As a default, we use [[bigglue.connectors.instances.IncrTrackerJobQueue]].
    * @param conf The configuration file needed to initialize.
    * @param builder The builder that created the platform. This was called with [[bigglue.computations.Mapper.init]] or [[bigglue.computations.Reducer.init]]
    */
  def initConnector(conf: PipeConfig, builder: PlatformBuilder): Unit = {
    val upstreamConnector = builder.connector[Input]("unary-platform-connector")
    upstreamConnector.init(conf)
    upstreamConnector.registerPlatform(this)
    upstreamConnectorOpt = Some(upstreamConnector)
  }

  def getUpstreamConnector(): Connector[Input] = upstreamConnectorOpt match {
    case Some(upstreamConnector) => upstreamConnector
    case None => {
      throw new NotInitializedException("UnaryPlatform", "getUpstreamConnector()", None)
    }
  }

  def getProvenanceCurator(): ProvenanceCurator[Input,Output] = provenanceCuratorOpt match {
    case Some(provenanceCurator) => provenanceCurator
    case None => {
      throw new NotInitializedException("UnaryPlatform", "getProvenanceCurator()", None)
    }
  }

  def getErrorCurator(): ErrorCurator[Input] = errorCuratorOpt match {
    case Some(errorCurator) => errorCurator
    case None => {
      throw new NotInitializedException("UnaryPlatform", "getErrorCurator()", None)
    }
  }

  def getVersionCurator(): VersionCurator[Output] = versionCuratorOpt match {
    case Some(versionCurator) => versionCurator
    case None => {
      val comp = computationOpt.get
      val myVersion = if (comp.useFullVersion) comp.shaVersionOpt else comp.versionOpt
      val versionCurator = myVersion match {
        case None => new IdleVersionCurator[Output]
        case Some(version) => new StandardVersionCurator[Output](version)
      }
      versionCuratorOpt = Some(versionCurator)
      versionCurator
    }
  }

  def getInputMap(): DataStore[Input] = inputMapOpt match {
    case Some(inputMap) => inputMap
    case None => {
      throw new NotInitializedException("UnaryPlatform", "getInputMap()", None)
    }
  }

  def getOutputMap(): DataStore[Output] = outputMapOpt match {
    case Some(outputMap) => outputMap
    case None => {
      throw new NotInitializedException("UnaryPlatform", "getOutputMap()", None)
    }
  }

  def getInputs(): Seq[Input] = getUpstreamConnector().retrieveUp()

  /**
    * This is called by the computation's persist step.
    * In particular, this tends to see how much of the data set actually needs to be re-sent.
    * As of right now, it sends the entirety of the input map to the upstream connector, which
    * handles which things to send/re-send down the pipeline, unless there is a reducer with
    * nothing in the input file, or if there is nothing in the output file.
    * If there is nothing in the output file, the program assumes that a new data store has been created
    * and sends down all data.
    */
  override def persist(): Unit = {
    println("PERSIST")
    println(getInputMap().name, getOutputMap().name, getVersionCurator().thisVersion)
    println(getInputMap().all().length, getOutputMap().all().length)
    (getInputMap().all().length, getOutputMap().all().length) match{
      case (0, 0) => getUpstreamConnector().persist(getInputMap())
      case (0, _) => computationOpt match{
        case Some(r: Reducer[_]) => outputMapOpt.get.extract()
        case _ => getUpstreamConnector().persist(getInputMap())
      }
      case (_, 0) => getUpstreamConnector().sendDown(inputMapOpt.get.all())
      case (_, _) => getUpstreamConnector().persist(getInputMap())
    }
    /*val inputMap = getInputMap().all().map(input => input.identity()->input).toMap
    getOutputMap().all().foldRight((List[Input](), Map[Input, String]())){
      case (output, (doNotSendDown, sendDownModified)) => output.getEmbedded("provInfo") match{
        case None => (doNotSendDown, sendDownModified)
        case Some(value: String) =>
          inputMap.get(Identity.deserialize[Input](value)) match{
            case None => (doNotSendDown, sendDownModified)
            case Some(i: Input) =>
              val currentIdentity = output.identity()
              val currVersionIdentity = getVersionCurator().stampVersion(currentIdentity)
              (currentIdentity.getVersion(), currVersionIdentity.getVersion()) match {
                case (None, None) => (i :: doNotSendDown, sendDownModified-i)
                case (Some(x), Some(y)) if x.equals(y) => (i :: doNotSendDown, sendDownModified-i)
                case (Some(x), None) if !doNotSendDown.contains(i) =>
                  (doNotSendDown, sendDownModified+(i->""))
                case (None, Some(x)) if !doNotSendDown.contains(i) =>
                  (doNotSendDown, sendDownModified+(i->""))
                case _ => (doNotSendDown, sendDownModified)
              }
          }
      }
    }*/
  }

  def updatedVersion(ver: Option[String]): Boolean = {
    //NOTE: AS OF THIS VERSION, THIS ONLY WORKS WITH STATIC VERSIONS.
    outputMapOpt match{
      case None => true
      case Some(outputMap) =>
        val a = outputMap.all()
        if (a.isEmpty) true else{
          a.head match{
            case out => (out.identity().getVersion(), ver) match{
              case (None, None) => true
              case (Some(x), Some(y)) if x.equals(y) => true
              case _ => false
            }
          }
        }
    }
  }

}

abstract class BinaryPlatform[InputL <: Identifiable[InputL],InputR <: Identifiable[InputR],Output <: Identifiable[Output]]
     extends Platform with BinaryChecker[InputL,InputR,Output] {

  var upstreamLConnectorOpt: Option[Connector[InputL]] = None
  var upstreamRConnectorOpt: Option[Connector[InputR]] = None
  var pairConnectorOpt: Option[Connector[bigglue.data.Pair[InputL,InputR]]] = None

  var provenanceCuratorOpt: Option[ProvenanceCurator[bigglue.data.Pair[InputL,InputR],Output]] = None
  var errorLeftCuratorOpt: Option[ErrorCurator[InputL]] = None
  var errorRightCuratorOpt: Option[ErrorCurator[InputR]] = None
  var errorPairCuratorOpt: Option[ErrorCurator[bigglue.data.Pair[InputL,InputR]]] = None
  var versionCuratorOpt: Option[VersionCurator[Output]] = None

  var inputLMapOpt: Option[DataStore[InputL]] = None
  var inputRMapOpt: Option[DataStore[InputR]] = None
  var outputMapOpt: Option[DataStore[Output]] = None

  def init(conf: PipeConfig, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    inputLMapOpt = Some(inputLMap)
    inputRMapOpt = Some(inputRMap)
    outputMapOpt = Some(outputMap)
    val thisVer = trueVersion(s"${inputLMap.sha}|${inputRMap.sha}")
    outputMap.setSha(thisVer)
    computationOpt match{
      case Some(comp) => comp.shaVersionOpt = Some(thisVer)
      case None => throw new Exception("Expected a Computation")
    }
    getVersionCurator()
    initConnectors(conf, builder)
    provenanceCuratorOpt = Some(builder.provenanceCurator)
    errorLeftCuratorOpt  = Some(builder.errorCurator)
    errorRightCuratorOpt = Some(builder.errorCurator)
    errorPairCuratorOpt  = Some(builder.errorCurator)
  }

  def initConnectors(conf: PipeConfig, builder: PlatformBuilder): Unit = {
    val upstreamLConnector = builder.connector[InputL]("binary-platform-connector-left")
    upstreamLConnector.init(conf)
    upstreamLConnectorOpt = Some(upstreamLConnector)
    val upstreamRConnector = builder.connector[InputR]("binary-platform-connector-right")
    upstreamRConnector.init(conf)
    upstreamRConnectorOpt = Some(upstreamRConnector)
    val pairConnector = builder.connector[bigglue.data.Pair[InputL,InputR]]("binary-platform-connector-pair")
    pairConnector.init(conf)
    pairConnectorOpt = Some(pairConnector)
  }

  def getUpstreamLConnector(): Connector[InputL] = upstreamLConnectorOpt match {
    case Some(upstreamConnector) => upstreamConnector
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getUpstreamLConnector()", None)
    }
  }

  def getUpstreamRConnector(): Connector[InputR] = upstreamRConnectorOpt match {
    case Some(upstreamConnector) => upstreamConnector
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getUpstreamRConnector()", None)
    }
  }

  def getPairConnector(): Connector[bigglue.data.Pair[InputL,InputR]] = pairConnectorOpt match {
    case Some(pairConnector) => pairConnector
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getPairConnector()", None)
    }
  }

  def getProvenanceCurator(): ProvenanceCurator[bigglue.data.Pair[InputL,InputR],Output] = provenanceCuratorOpt match {
    case Some(provenanceCurator) => provenanceCurator
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getProvenanceCurator()", None)
    }
  }

  def getVersionCurator(): VersionCurator[Output] = versionCuratorOpt match {
    case Some(versionCurator) => versionCurator
    case None => {
      val versionCurator = computationOpt.get.shaVersionOpt match {
        case None => new IdleVersionCurator[Output]
        case Some(version) => new StandardVersionCurator[Output](version)
      }
      versionCuratorOpt = Some(versionCurator)
      versionCurator
    }
  }

  def getLeftErrorCurator(): ErrorCurator[InputL] = errorLeftCuratorOpt match {
    case Some(errorCurator) => errorCurator
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getLeftErrorCurator()", None)
    }
  }

  def getRightErrorCurator(): ErrorCurator[InputR] = errorRightCuratorOpt match {
    case Some(errorCurator) => errorCurator
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getRightErrorCurator()", None)
    }
  }

  def getPairErrorCurator(): ErrorCurator[bigglue.data.Pair[InputL,InputR]] = errorPairCuratorOpt match {
    case Some(errorCurator) => errorCurator
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getPairErrorCurator()", None)
    }
  }

  def getInputLMap(): DataStore[InputL] = inputLMapOpt match {
    case Some(inputMap) => inputMap
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getInputLMap()", None)
    }
  }

  def getInputRMap(): DataStore[InputR] = inputRMapOpt match {
    case Some(inputMap) => inputMap
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getInputRMap()", None)
    }
  }

  def getOutputMap(): DataStore[Output] = outputMapOpt match {
    case Some(outputMap) => outputMap
    case None => {
      throw new NotInitializedException("BinaryPlatform", "getOutputMap()", None)
    }
  }

  def getInputs(): (Seq[InputL],Seq[InputR],Seq[bigglue.data.Pair[InputL,InputR]]) = {

     val inputLs = getUpstreamLConnector().retrieveUp()
     val inputRs = getUpstreamRConnector().retrieveUp()

     val inputPairs = (
       inputLs.map(
         inputL => getInputRMap().all().map(
           inputR => bigglue.data.Pair(inputL,inputR)
         )
       ) ++
       inputRs.map(
         inputR => getInputLMap().all().map(
           inputL => bigglue.data.Pair(inputL,inputR)
         )
       )
     ).flatten.toSet

     getPairConnector().sendDown(inputPairs.toSeq)

     val newInputPairs = getPairConnector().retrieveUp()

    (inputLs,inputRs,newInputPairs)
  }

  override def persist(): Unit = {
    getUpstreamLConnector().persist(getInputLMap())
    getUpstreamRConnector().persist(getInputRMap())
  }

}