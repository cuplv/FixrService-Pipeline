package protopipes.platforms

import protopipes.builders.PlatformBuilder
import protopipes.connectors.Connector
import protopipes.data.Identifiable
import protopipes.store.DataStore
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */


abstract class Platform {

  def terminate(): Unit

  def run(): Unit

  def wake(): Unit

}

abstract class UnaryPlatform[Input <: Identifiable[Input],Output] extends Platform {

  var upstreamConnectorOpt: Option[Connector[Input]] = None
  var inputMapOpt: Option[DataStore[Input]]   = None
  var outputMapOpt: Option[DataStore[Output]] = None

  def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    inputMapOpt = Some(inputMap)
    outputMapOpt = Some(outputMap)
    initConnector(conf, builder)
  }

  def initConnector(conf: Config, builder: PlatformBuilder): Unit = {
    val upstreamConnector = builder.connector[Input]("unary-platform-connector")
    upstreamConnector.init(conf)
    upstreamConnector.registerPlatform(this)
    upstreamConnectorOpt = Some(upstreamConnector)
  }

  def getUpstreamConnector(): Connector[Input] = upstreamConnectorOpt match {
    case Some(upstreamConnector) => upstreamConnector
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getInputMap(): DataStore[Input] = inputMapOpt match {
    case Some(inputMap) => inputMap
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getOutputMap(): DataStore[Output] = outputMapOpt match {
    case Some(outputMap) => outputMap
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getInputs(): Seq[Input] = getUpstreamConnector().retrieveUp()

}

abstract class BinaryPlatform[InputL <: Identifiable[InputL],InputR <: Identifiable[InputR],Output] extends Platform {

  var upstreamLConnectorOpt: Option[Connector[InputL]] = None
  var upstreamRConnectorOpt: Option[Connector[InputR]] = None
  var pairConnectorOpt: Option[Connector[protopipes.data.Pair[InputL,InputR]]] = None
  var inputLMapOpt: Option[DataStore[InputL]] = None
  var inputRMapOpt: Option[DataStore[InputR]] = None
  var outputMapOpt: Option[DataStore[Output]] = None

  def init(conf: Config, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    inputLMapOpt = Some(inputLMap)
    inputRMapOpt = Some(inputRMap)
    outputMapOpt = Some(outputMap)
    initConnectors(conf, builder)
  }

  def initConnectors(conf: Config, builder: PlatformBuilder): Unit = {
    val upstreamLConnector = builder.connector[InputL]("binary-platform-connector-left")
    upstreamLConnector.init(conf)
    upstreamLConnectorOpt = Some(upstreamLConnector)
    val upstreamRConnector = builder.connector[InputR]("binary-platform-connector-right")
    upstreamRConnector.init(conf)
    upstreamRConnectorOpt = Some(upstreamRConnector)
    val pairConnector = builder.connector[protopipes.data.Pair[InputL,InputR]]("binary-platform-connector-pair")
    pairConnector.init(conf)
    pairConnectorOpt = Some(pairConnector)
  }

  def getUpstreamLConnector(): Connector[InputL] = upstreamLConnectorOpt match {
    case Some(upstreamConnector) => upstreamConnector
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getUpstreamRConnector(): Connector[InputR] = upstreamRConnectorOpt match {
    case Some(upstreamConnector) => upstreamConnector
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getPairConnector(): Connector[protopipes.data.Pair[InputL,InputR]] = pairConnectorOpt match {
    case Some(pairConnector) => pairConnector
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getInputLMap(): DataStore[InputL] = inputLMapOpt match {
    case Some(inputMap) => inputMap
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getInputRMap(): DataStore[InputR] = inputRMapOpt match {
    case Some(inputMap) => inputMap
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getOutputMap(): DataStore[Output] = outputMapOpt match {
    case Some(outputMap) => outputMap
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getInputs(): (Seq[InputL],Seq[InputR],Seq[protopipes.data.Pair[InputL,InputR]]) = {

     val inputLs = getUpstreamLConnector().retrieveUp()
     val inputRs = getUpstreamRConnector().retrieveUp()

     val inputPairs = (
       inputLs.map(
         inputL => getInputRMap().all().map(
           inputR => protopipes.data.Pair(inputL,inputR)
         )
       ) ++
       inputRs.map(
         inputR => getInputLMap().all().map(
           inputL => protopipes.data.Pair(inputL,inputR)
         )
       )
     ).flatten.toSet

     getPairConnector().sendDown(inputPairs.toSeq)

     val newInputPairs = getPairConnector().retrieveUp()

    (inputLs,inputRs,newInputPairs)
  }

}