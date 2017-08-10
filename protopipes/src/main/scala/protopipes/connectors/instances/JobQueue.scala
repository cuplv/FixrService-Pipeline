package protopipes.connectors.instances

import protopipes.builders.PlatformBuilder
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
    getDownstream().signalDown()
  }

  override def retrieveUp(): Seq[Data] = queue.extract()

  override def reportUp(status:Status, data: Seq[Identity[Data]]): Unit = { }

  override def size(): Int = queue.size()

}

class IncrTrackerJobQueue[Data <: Identifiable[Data]] extends JobQueue[Data] {

  // var currId: Id = 0L
  // val history: DataMap[Id, Identity[Data]] = new InMemDataMap[Id, Identity[Data]]
  val statusMap: DataMultiMap[Status, Identity[Data]] = new InMemDataMultiMap[Status, Identity[Data]]

  def getMap(): IdDataMap[Data] = datastoreOpt match {
    case Some(dataStore) => dataStore.asInstanceOf[IdDataMap[Data]]
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def getStatusMap(): DataMultiMap[Status, Identity[Data]] = statusMap

  override def sendDown(data: Seq[Data]): Unit = {
    val newData = data.filterNot( d => statusMap.contains(Status.Done, d.asInstanceOf[Identifiable[Data]].identity()) )
    statusMap.put(Status.NotDone, newData.map( _.identity() ).toSet)
    super.sendDown(newData)
  }

  override def retrieveUp(): Seq[Data] = {
    queue.extract() ++ (statusMap.get(Status.Error) ++ statusMap.get(Status.Modified)).flatMap( getMap().get(_) )
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

  override def reportUp(status: Status, ids: Seq[Identity[Data]]): Unit = {
    statusMap.remove( ids.toSet )
    statusMap.put(status, ids.toSet )
    // history.remove(ids)
  }

}