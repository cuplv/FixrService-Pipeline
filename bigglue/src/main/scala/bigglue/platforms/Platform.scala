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

/**
  * Created by edmundlam on 8/8/17.
  */


abstract class Platform {

  var computationOpt: Option[Computation] = None

  def setComputation(computation: Computation): Platform = {
    computationOpt = Some(computation)
    this
  }

  def terminate(): Unit

  def run(): Unit = computationOpt match {
    case Some(computation) => computation.run()
    case None => {
      throw new NotInitializedException("Platform", "run()", None)
    }
  }

  def persist(): Unit

  def wake(): Unit

}

abstract class UnaryPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]] extends Platform with UnaryChecker[Input,Output] {

  var upstreamConnectorOpt: Option[Connector[Input]] = None
  var provenanceCuratorOpt: Option[ProvenanceCurator[Input,Output]] = None
  var errorCuratorOpt: Option[ErrorCurator[Input]] = None
  var versionCuratorOpt: Option[VersionCurator[Output]] = None
  var inputMapOpt: Option[DataStore[Input]]   = None
  var outputMapOpt: Option[DataStore[Output]] = None

  def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    inputMapOpt = Some(inputMap)
    outputMapOpt = Some(outputMap)
    initConnector(conf, builder)
    provenanceCuratorOpt = Some(builder.provenanceCurator)
    errorCuratorOpt = Some(builder.errorCurator)
  }

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
      val versionCurator = computationOpt.get.versionOpt match {
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

  override def persist(): Unit = {
    getUpstreamConnector().persist(getInputMap())
    val inputMap = getInputMap().all().map(input => input.identity()->input).toMap
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
      val versionCurator = computationOpt.get.versionOpt match {
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