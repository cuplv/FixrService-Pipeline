package bigglue.connectors.instances

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import bigglue.configurations.PipeConfig
import bigglue.connectors.Connector
import bigglue.connectors.Status.Status
import bigglue.exceptions.NotInitializedException
import bigglue.platforms.Platform
import bigglue.store.DataStore
import bigglue.store.instances.{InMemDataQueue, InMemLinearStore}

import scala.concurrent.duration._

/**
  * Created by chanceroberts on 11/16/17.
  */

case class OpenTheFloodGates(send: Int = 0)

class WaitingActor[Data](connector: WaitingConnector[Data]) extends Actor {
  override def receive: Receive = {
    case OpenTheFloodGates(send) => try{
      connector.reallySendDown(send)
    } catch{
      case e: Exception => throw new Exception(s"Problem with ${connector.name}: ${connector.floodList.all()}")
    }
    case _ => ()
  }
}

class WaitingConnector[Data]() extends Connector[Data]{
  var connectorOpt: Option[Connector[Data]] = None
  def connector: Connector[Data] = connectorOpt match{
    case None => throw new NotInitializedException("WaitingConnector", "InnerConnector", None)
    case Some(c) => c
  }

  def unique: Boolean = uniqueOpt match {
    case None => throw new NotInitializedException("WaitingConnector", "onlyUnique", None)
    case Some(x) => x
  }

  val floodList: InMemDataQueue[(Boolean, Data)] = new InMemDataQueue[(Boolean, Data)]
  // Do we really want a separate ActorSystem for this? :(
  val actorSystem: ActorSystem = ActorSystem.create()
  implicit val dispatcher = actorSystem.dispatcher
  val floodGates: ActorRef = actorSystem.actorOf(Props(classOf[WaitingActor[Data]], this))
  var uniqueOpt: Option[Boolean] = None
  var name: String = ""

  override def init(conf: PipeConfig): Unit = {
    val trueConfig = conf.typeSafeConfig
    // To be changed? :|
    val connConf = trueConfig.getObject("bigglue").toConfig.getObject("waitingConnector").toConfig
    connectorOpt = Some(Class.forName(connConf.getString("innerConnector"))
      .getConstructors()(0).newInstance().asInstanceOf[Connector[Data]])
    val delay = connConf.getDouble("delayInSeconds").seconds // 2.seconds
    val send = connConf.getInt("amountPerInterval") // 0
    val interval = connConf.getDouble("intervalsInSeconds").seconds // 2.seconds
    uniqueOpt = Some(connConf.getBoolean("onlyUnique"))
    actorSystem.scheduler.schedule(delay, interval, floodGates, OpenTheFloodGates(send))
  }

  override def retrieveUp(): Seq[Data] = connector.retrieveUp()

  override def registerPlatform(platform: Platform): Unit = {
    super.registerPlatform(platform)
    connector.registerPlatform(platform)
    name = s"Platform $platform: Connector ${connector.getDownstream()}"
    println(name)
  }

  override def registerDownstreamConnector(connector: Connector[Data]): Unit = {
    super.registerDownstreamConnector(connector)
    connector.registerDownstreamConnector(connector)
  }

  override def registerUpstreamConnector(connector: Connector[Data]): Unit = {
    super.registerUpstreamConnector(connector)
    connector.registerUpstreamConnector(connector)
  }

  def reallySendDown(send: Int): Unit = {
    val (notModified, modified, _) = (send match{
      case 0 => floodList.extract()
      case x =>
        def createList(left: Int, list: List[(Boolean, Data)] = List()): List[(Boolean, Data)] = {
          if (left == 0) list
          else floodList.dequeue() match{
            case Some(x: (Boolean, Data)) => x :: list
            case _ => list
          }
        }
        createList(send).reverse
    }).foldRight(List[Data](), List[Data](), Map[Data, Boolean]()){
      case ((false, nMData), (nModified, iModified, newData)) =>
        if (unique) newData.get(nMData) match{
          case None => (nMData :: nModified, iModified, newData+(nMData->true))
          case Some(_) =>
            floodList.put((false, nMData))
            (nModified, iModified, newData)
        }
        else (nMData :: nModified, iModified, newData+(nMData->true))
      case ((true, mData), (nModified, iModified, newData)) =>
        if (unique) newData.get(mData) match{
          case None => (nModified, mData :: iModified, newData+(mData->false))
          case Some(_) =>
            floodList.put((true, mData))
            (nModified, iModified, newData)
        }
        else (nModified, mData :: iModified, newData+(mData->false))
    }
    if (notModified.nonEmpty) connector.sendDown(notModified)
    if (modified.nonEmpty) connector.sendDownModified(modified)
    if (notModified.nonEmpty || modified.nonEmpty) connector.signalDown()
  }

  override def sendDownModified(data: Seq[Data]): Unit = data.foreach(input => floodList.put((true, input)))

  override def sendDown(data: Seq[Data]): Unit = data.foreach(input => floodList.put((false, input)))

  override def signalDown(): Unit = ()

  override def reportUp(status: Status, data: Seq[Data]): Unit = connector.reportUp(status, data)

  override def size(): Int = floodList.ls.size

  override def terminate(): Unit = actorSystem.terminate()
}
