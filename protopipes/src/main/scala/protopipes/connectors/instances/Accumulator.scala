package protopipes.connectors.instances

import protopipes.connectors.Connector
import protopipes.connectors.Connector.Id
import protopipes.connectors.Status.Status
import protopipes.data.Identity
import protopipes.store.DataStore
import protopipes.store.instances.InMemLinearStore
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/10/17.
  */
case class Accumulator[Data](limit: Int = 50) extends Connector[Data] {

  val dataStore: DataStore[Data] = new InMemLinearStore[Data]

  override def init(conf: Config): Unit = {}

  override def terminate(): Unit = {}

  override def signalDown(): Unit = {
    if (dataStore.size >= limit) {
      getDownstream().signalDown()
    }
  }

  override def sendDown(data: Seq[Data]): Unit = {
     dataStore.put(data)
     signalDown()
  }

  override def sendDownModified(data: Seq[Data]): Unit = sendDown(data)

  override def retrieveUp(): Seq[Data] = {
     dataStore.extract()
  }

  override def reportUp(status: Status, data: Seq[Data]): Unit = upstreamConnectorOpt match {
    case Some(upstreamConnector) => upstreamConnector.reportUp(status, data)
    case None => // Do nothing
  }

  override def size(): Int = dataStore.size()

}
