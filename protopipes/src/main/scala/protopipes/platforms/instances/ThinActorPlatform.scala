package protopipes.platforms.instances

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import protopipes.builders.PlatformBuilder
import protopipes.computations.Mapper
import protopipes.connectors.Connector
import protopipes.connectors.Connector.Id
import protopipes.connectors.instances.ActorConnector
import protopipes.data.Identifiable
import protopipes.platforms.{ComputesMap, UnaryPlatform}
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

class ThinActorUnaryPlatformActor[Input <: Identifiable[Input], Output](platform: UnaryPlatform[Input, Output]) extends Actor {

  override def receive: Receive = {
    case Wake() => platform.run()
  }

}


abstract class ThinActorUnaryPlatform[Input <: Identifiable[Input], Output](name: String = ThinActorPlatform.NAME) extends UnaryPlatform[Input, Output] {

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
     val actorRef = actorSystem.actorOf(Props(classOf[ThinActorUnaryPlatformActor[Input,Output]], this), name = ThinActorPlatform.NAME)
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

class ThinActorMapperPlatform[Input <: Identifiable[Input], Output](name: String = ThinActorPlatform.NAME + Random.nextInt(99999)) extends ThinActorUnaryPlatform[Input, Output] with ComputesMap[Input, Output] {

  override def run(): Unit = {
    val mapper = getMapper()
    val upstreamConnector = getUpstreamConnector()
    val outputMap = getOutputMap()
    upstreamConnector.retrieveUp() foreach {
      input => tryComputeThenStore(upstreamConnector, mapper, input, outputMap)
    }

  }

}