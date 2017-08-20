package protopipes.connectors.instances

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import protopipes.connectors.Connector
import protopipes.connectors.Connector.Id
import protopipes.connectors.Status.Status
import protopipes.connectors.instances.ActorConnectorActor._
import com.typesafe.config.Config
import akka.pattern.ask
import protopipes.configurations.PipeConfig
import protopipes.data.{BasicIdentity, Identifiable}
import protopipes.exceptions.NotInitializedException
import protopipes.platforms.Platform

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by edmundlam on 8/8/17.
  */

object ActorConnectorActor {

  val NAME: String = "connector-actor"

  case class SendDown[Data](data: Seq[Data])

  case class SendDownModified[Data](data: Seq[Data])

  case class RetrieveUp()

  case class ReportUp[Data](status: Status, ids: Seq[Data])

  case class Size()

}

class ActorConnectorActor[Data](connector: Connector[Data]) extends Actor {

  override def receive: Receive = {

    case SendDown(data: Seq[Data]) => connector.sendDown(data)

    case SendDownModified(data: Seq[Data]) => connector.sendDownModified(data)

    case RetrieveUp() => sender() ! connector.retrieveUp()

    case ReportUp(status: Status, ids: Seq[Data]) => connector.reportUp(status, ids)

    case Size() => sender() ! connector.size()
  }
}

abstract class ActorConnector[Data](name: String = ActorConnectorActor.NAME)(implicit actorSystem: ActorSystem) extends Connector[Data] {

  val innerConnector: Connector[Data]

  var actorRefOpt: Option[ActorRef] = None

  def getName(): String = name

  def getActor(): ActorRef = actorRefOpt match {
    case Some(actorRef) => actorRef
    case None => {
      throw new NotInitializedException("ActorConnector", "getActor()", None)
    }
  }

  override def init(conf: PipeConfig): Unit = {
     innerConnector.init(conf)
     val actorRef = actorSystem.actorOf(Props(classOf[ActorConnectorActor[Data]], innerConnector), name = name)
     actorRefOpt = Some(actorRef)
  }

  override def registerPlatform(platform: Platform): Unit = {
    super.registerPlatform(platform)
    innerConnector.registerPlatform(platform)
  }

  override def terminate(): Unit = { }

  override def signalDown(): Unit = {  }

  override def sendDown(data: Seq[Data]): Unit = getActor ! SendDown(data)

  override def sendDownModified(data: Seq[Data]): Unit = getActor ! SendDownModified(data)

  override def retrieveUp(): Seq[Data] = {
    println(s"$name retrieving upstream...")
    implicit val timeout: Timeout = 60 seconds
    val future = getActor ? RetrieveUp()
    val res = Await.result(future, timeout.duration).asInstanceOf[Seq[Data]]
    println(s"$name retrieved results: $res")
    res
  }

  override def reportUp(status: Status, data: Seq[Data]): Unit = {
    println(s"$name reporting $status -> $data")
    getActor ! ReportUp(status, data)
  }

  override def size(): Int = {
    implicit val timeout: Timeout = 60 seconds
    val future = getActor ? Size()
    Await.result(future, timeout.duration).asInstanceOf[Int]
  }

}


class ActorIncrTrackerJobQueue[Data <: Identifiable[Data]](name: String)(implicit actorSystem: ActorSystem)
  extends ActorConnector[Data](name) {

  override val innerConnector: Connector[Data] = new IncrTrackerJobQueue

}
