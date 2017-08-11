package protopipes.platforms.instances

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import protopipes.configurations.PlatformBuilder
import protopipes.computations.Mapper
import protopipes.connectors.{Connector, Status}
import protopipes.connectors.Connector.Id
import protopipes.connectors.instances.ActorConnector
import protopipes.data.Identifiable
import protopipes.platforms._
import protopipes.platforms.instances.ThinActorPlatform.Wake
import protopipes.store.DataStore
import com.typesafe.config.Config

import scala.util.Random

/**
  * Created by edmundlam on 8/8/17.
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


abstract class ThinActorUnaryPlatform[Input <: Identifiable[Input], Output](name: String = ThinActorPlatform.NAME + s"-unary-${Random.nextInt(99999)}") extends UnaryPlatform[Input, Output] {

  var actorRefOpt: Option[ActorRef] = None

  implicit val actorSystem = ActorSystem(name)

  def getActor(): ActorRef = actorRefOpt match {
    case Some(actorRef) => actorRef
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  override def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
     super.init(conf, inputMap, outputMap, builder)
     val actorRef = actorSystem.actorOf(Props(classOf[ThinActorPlatformActor], this), name = ThinActorPlatform.NAME)
     actorRefOpt = Some(actorRef)
  }

  override def initConnector(conf: Config, builder: PlatformBuilder): Unit = {
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

abstract class ThinActorBinaryPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output]
   (name: String = ThinActorPlatform.NAME + s"-binary-${Random.nextInt(99999)}") extends BinaryPlatform[InputL,InputR, Output] {

  var actorRefOpt: Option[ActorRef] = None

  implicit val actorSystem = ActorSystem(name)

  def getActor(): ActorRef = actorRefOpt match {
    case Some(actorRef) => actorRef
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  override def init(conf: Config, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputLMap, inputRMap, outputMap, builder)
    val actorRef = actorSystem.actorOf(Props(classOf[ThinActorPlatformActor], this), name = ThinActorPlatform.NAME)
    actorRefOpt = Some(actorRef)
  }

  override def initConnectors(conf: Config, builder: PlatformBuilder): Unit = {
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

class ThinActorMapperPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
     (name: String = ThinActorPlatform.NAME + Random.nextInt(99999)) extends ThinActorUnaryPlatform[Input, Output] with ComputesMap[Input, Output] {

  override def run(): Unit = {
    val mapper = getMapper()
    val upstreamConnector = getUpstreamConnector()
    val outputMap = getOutputMap()
    getInputs() foreach {
      input => tryComputeThenStore(upstreamConnector, mapper, input, outputMap)
    }

  }

}

class ThinActorPairwiseComposerPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR],Output <: Identifiable[Output]]
        (name: String = ThinActorPlatform.NAME + Random.nextInt(99999)) extends ThinActorBinaryPlatform[InputL,InputR,Output]  with ComputesPairwiseCompose[InputL,InputR,Output] {

  override def run(): Unit = {
    val composer = getComposer()
    val pairConnector = getPairConnector()
    val outputMap = getOutputMap()
    val inputs = getInputs()
    inputs._3 foreach {
      tryComposeThenStore(pairConnector, composer, _, outputMap)
    }
    getUpstreamLConnector().reportUp(Status.Done,inputs._1)
    getUpstreamRConnector().reportUp(Status.Done,inputs._2)
  }

}