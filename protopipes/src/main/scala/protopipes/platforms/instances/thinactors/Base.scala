package protopipes.platforms.instances.thinactors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import protopipes.configurations.{PipeConfig, PlatformBuilder}
import protopipes.connectors.Connector
import protopipes.connectors.instances.ActorConnector
import protopipes.data.Identifiable
import protopipes.exceptions.NotInitializedException
import protopipes.platforms.{BinaryPlatform, Platform, UnaryPlatform}
import protopipes.platforms.instances.thinactors.ThinActorPlatform.Wake
import protopipes.store.DataStore

import scala.util.Random

/**
  * Created by edmundlam on 8/11/17.
  */

object ThinActorPlatform {

  val NAME: String = "platform-actor"

  case class Wake()

}

class ThinActorPlatformActor(platform: Platform) extends Actor {

  override def receive: Receive = {
    case Wake() => platform.run()
  }

}


abstract class ThinActorUnaryPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = ThinActorPlatform.NAME + s"-unary-${Random.nextInt(99999)}") extends UnaryPlatform[Input, Output] {

  var actorRefOpt: Option[ActorRef] = None

  implicit val actorSystem = ActorSystem(name)

  def getActor(): ActorRef = actorRefOpt match {
    case Some(actorRef) => actorRef
    case None => {
      throw new NotInitializedException("ThinActorUnaryPlatform", "getActor()", None)
    }
  }

  override def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputMap, outputMap, builder)
    val actorRef = actorSystem.actorOf(Props(classOf[ThinActorPlatformActor], this), name = ThinActorPlatform.NAME)
    actorRefOpt = Some(actorRef)
  }

  override def initConnector(conf: PipeConfig, builder: PlatformBuilder): Unit = {
    // println("Called this")
    val upstreamConnector = new ActorConnector[Input] {
      override val innerConnector: Connector[Input] = builder.connector[Input]("connector")
    }
    upstreamConnector.init(conf)
    upstreamConnector.registerPlatform(this)
    upstreamConnectorOpt = Some(upstreamConnector)
  }

  override def wake(): Unit = getActor() ! Wake()

  override def terminate(): Unit = actorSystem.terminate()

}

abstract class ThinActorBinaryPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]]
(name: String = ThinActorPlatform.NAME + s"-binary-${Random.nextInt(99999)}") extends BinaryPlatform[InputL,InputR, Output] {

  var actorRefOpt: Option[ActorRef] = None

  implicit val actorSystem = ActorSystem(name)

  def getActor(): ActorRef = actorRefOpt match {
    case Some(actorRef) => actorRef
    case None => {
      throw new NotInitializedException("ThinActorBinaryPlatform", "getActor()", None)
    }
  }

  override def init(conf: PipeConfig, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputLMap, inputRMap, outputMap, builder)
    val actorRef = actorSystem.actorOf(Props(classOf[ThinActorPlatformActor], this), name = ThinActorPlatform.NAME)
    actorRefOpt = Some(actorRef)
  }

  override def initConnectors(conf: PipeConfig, builder: PlatformBuilder): Unit = {
    // println("Called this")
    val upstreamLConnector = new ActorConnector[InputL]("binary-platform-connector-left")(actorSystem) {
      override val innerConnector: Connector[InputL] = builder.connector[InputL]("binary-platform-connector-left")
    }
    upstreamLConnector.init(conf)
    upstreamLConnector.registerPlatform(this)
    upstreamLConnectorOpt = Some(upstreamLConnector)
    val upstreamRConnector = new ActorConnector[InputR]("binary-platform-connector-right")(actorSystem) {
      override val innerConnector: Connector[InputR] = builder.connector[InputR]("binary-platform-connector-right")
    }
    upstreamRConnector.init(conf)
    upstreamRConnector.registerPlatform(this)
    upstreamRConnectorOpt = Some(upstreamRConnector)
    val pairConnector = new ActorConnector[protopipes.data.Pair[InputL,InputR]]("binary-platform-connector-pair")(actorSystem) {
      override val innerConnector: Connector[protopipes.data.Pair[InputL,InputR]] = builder.connector[protopipes.data.Pair[InputL,InputR]]("binary-platform-connector-pair")
    }
    pairConnector.init(conf)
    // pairConnector.registerPlatform(this)
    pairConnectorOpt = Some(pairConnector)
  }

  override def wake(): Unit = getActor() ! Wake()

  override def terminate(): Unit = actorSystem.terminate()

}
