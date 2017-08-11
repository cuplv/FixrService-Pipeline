package protopipes.connectors.instances

import protopipes.configurations.PlatformBuilder
import protopipes.connectors.{Connector, Status}
import protopipes.connectors.Connector.Id
import protopipes.connectors.Status.Status
import protopipes.data.{Identifiable, Identity}
import protopipes.store.{DataMap, DataMultiMap, DataQueue, IdDataMap}
import protopipes.store.instances.{InMemDataMap, InMemDataMultiMap, InMemDataQueue}
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */
class JobQueue[Data] extends Connector[Data] {

  var queue: DataQueue[Data] = new InMemDataQueue[Data]

  override def init(conf: Config): Unit = { }

  override def terminate(): Unit = { }

  override def signalDown(): Unit = getDownstream().signalDown()

  override def sendDown(data: Seq[Data]): Unit = {
    queue.enqueue(data)
    // getPlatform().wake()
    // getDownstream().signalDown()
    super.signalDown()
  }

  override def retrieveUp(): Seq[Data] = queue.extract()

  override def reportUp(status:Status, data: Seq[Data]): Unit = { }

  override def size(): Int = queue.size()

}

class IncrTrackerJobQueue[Data <: Identifiable[Data]] extends JobQueue[Data] {

  // var currId: Id = 0L
  // val history: DataMap[Id, Identity[Data]] = new InMemDataMap[Id, Identity[Data]]
  val statusMap: DataMultiMap[Status, Data] = new InMemDataMultiMap[Status, Data]

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

  override def retrieveUp(): Seq[Data] = {
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
    // history.remove(ids)
  }

}