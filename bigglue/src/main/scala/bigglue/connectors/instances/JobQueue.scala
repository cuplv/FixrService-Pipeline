package bigglue.connectors.instances

import bigglue.configurations.{PipeConfig, PlatformBuilder}
import bigglue.connectors.{Connector, Status}
import bigglue.connectors.Connector.Id
import bigglue.connectors.Status.Status
import bigglue.data.{BasicIdentity, I, Identifiable}
import bigglue.exceptions.{NotInitializedException, ProtoPipeException}
import bigglue.platforms.Platform
import bigglue.store.instances.file.TextFileDataMap
import bigglue.store.{DataMap, DataMultiMap, DataQueue, IdDataMap}
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
  var name: String = ""

  /*
  def storedMap: DataMap[I[Int], I[String]] = storedMapOpt match{
    case Some(x) => x
    case None => throw new ProtoPipeException(Some("Stored Map call not allowed: Platform has not been registered."))
  }
  */

  /*
  def getMap(): DataMap[Identity[Data],Data] = datastoreOpt match {
    case Some(dataStore) => dataStore.asInstanceOf[DataMap[Identity[Data],Data]]
    case None => {
      // TODO: Throw exception
      ???
    }
  } */

  def getStatusMap(): DataMultiMap[Status, Data] = statusMap

  override def sendDown(data: Seq[Data]): Unit = {
    val newData = data.filterNot( d => statusMap.contains(Status.Done, d) )
    statusMap.put(Status.NotDone, newData.toSet)
    super.sendDown(newData)
  }

  override def sendDownModified(data: Seq[Data]): Unit = {
    statusMap.put(Status.Modified, data.toSet)
    super.sendDown(data)
  }

  override def registerPlatform(platform: Platform): Unit = {
    super.registerPlatform(platform)
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
    println(statusMap)
    // history.remove(ids)
  }

}