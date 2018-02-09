package bigglue.connectors.instances

import bigglue.configurations.{PipeConfig, PlatformBuilder}
import bigglue.connectors.{Connector, Status}
import bigglue.connectors.Connector.Id
import bigglue.connectors.Status.Status
import bigglue.data.{BasicIdentity, I, Identifiable}
import bigglue.exceptions.{NotInitializedException, ProtoPipeException}
import bigglue.platforms.{BinaryPlatform, Platform, UnaryPlatform}
import bigglue.store.instances.file.TextFileDataMap
import bigglue.store._
import bigglue.store.instances.{InMemDataMap, InMemDataMultiMap, InMemDataQueue}
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */
class JobQueue[Data] extends Connector[Data] {

  var queue: DataQueue[Data] = new InMemDataQueue[Data]

  override def init(conf: PipeConfig): Unit = { }

  override def terminate(): Unit = { }

  override def signalDown(): Unit = getDownstream().signalDown()

  override def sendDown(data: Seq[Data]): Unit = {
    queue.enqueue(data)
    // getPlatform().wake()
    // getDownstream().signalDown()
    super.signalDown()
  }

  override def sendDownModified(data: Seq[Data]): Unit = sendDown(data)

  override def retrieveUp(): Seq[Data] = queue.extract()

  override def reportUp(status:Status, data: Seq[Data]): Unit = { }

  override def size(): Int = queue.size()

}

class IncrTrackerJobQueue[Data <: Identifiable[Data]] extends JobQueue[Data] {

  // var currId: Id = 0L
  // val history: DataMap[Id, Identity[Data]] = new InMemDataMap[Id, Identity[Data]]
  val statusMap: DataMultiMap[Status, Data] = new InMemDataMultiMap[Status, Data]
  var iMapOpt: Option[DataStore[Data]] = None

  def iMap: DataStore[Data] = iMapOpt match{
    case Some(x) => x
    case None => throw new ProtoPipeException(Some("Stored Map call not allowed: Platform has not been registered."))
  }

  /*
  def getMap(): DataMap[Identity[Data],Data] = datastoreOpt match {
    case Some(dataStore) => dataStore.asInstanceOf[DataMap[Identity[Data],Data]]
    case None => {
      // TODO: Throw exception
      ???
    }
  } */

  def getStatusMap(): DataMultiMap[Status, Data] = statusMap

  private def appendStatus(data: Seq[Data], status: Status): Unit = {
    data.foreach{dat =>
      dat.addEmbedded("status", status.toString)
    }
    //iMap.put_(data)
  }

  override def sendDown(data: Seq[Data]): Unit = {
    val newData = data.filterNot( d => statusMap.contains(Status.Done, d) )
    statusMap.put(Status.NotDone, newData.toSet)
    appendStatus(newData, Status.NotDone)
    super.sendDown(newData)
  }

  override def sendDownModified(data: Seq[Data]): Unit = {
    statusMap.put(Status.Modified, data.toSet)
    appendStatus(data, Status.Modified)
    super.sendDown(data)
  }

  override def registerPlatform(platform: Platform): Unit = {
    super.registerPlatform(platform)
   /* platform match{
      case u: UnaryPlatform[Data, _] =>
        iMapOpt = Some(u.getInputMap())
      case b: BinaryPlatform[_, _, _] =>
        iMapOpt = b.upstreamLConnectorOpt match {
          case None => b.inputLMapOpt.asInstanceOf[Option[DataStore[Data]]]
          case _ => b.upstreamRConnectorOpt match {
            case None => b.inputRMapOpt.asInstanceOf[Option[DataStore[Data]]]
            case _ => throw new ProtoPipeException(Some("Connector registered to full platform!"))
          }
        }
    }
    (try{
      val iterator = iMap.iterator()
      iterator
    } catch {
      case e: Exception => iMap.all()
    }).foreach{i =>
      i.getEmbedded("status") match{
        case Some(x) =>
          statusMap.put(Status.withName(x), List(i).toSet)
        case _ => ()
      }}
    sendDown(statusMap.get(Status.NotDone).toSeq)
    signalDown()*/
  }

  override def retrieveUp(): Seq[Data] = {
    // val unique = queue.extract().distinct
    queue.extract() ++ (statusMap.get(Status.Error) ++ statusMap.get(Status.Modified))
     /*
     (previous ++ queue.extract()) foreach {
       d => {
         out = out :+ (currId, d)
         history.put(currId, d.asInstanceOf[Identifiable[Data]].identity())
         currId = Connector.incr(currId)
       }
     }
     out */
  }

  override def reportUp(status: Status, data: Seq[Data]): Unit = {
    statusMap.remove( data.toSet )
    statusMap.put(status, data.toSet )
    appendStatus(data, status)
    // history.remove(ids)
  }

}