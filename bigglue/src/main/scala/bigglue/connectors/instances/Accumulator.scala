package bigglue.connectors.instances

import bigglue.connectors.Connector
import bigglue.connectors.Connector.Id
import bigglue.connectors.Status.Status
import bigglue.data.BasicIdentity
import bigglue.store.DataStore
import bigglue.store.instances.InMemLinearStore
import com.typesafe.config.Config
import bigglue.configurations.PipeConfig

/**
  * Created by edmundlam on 8/10/17.
  */
case class Accumulator[Data](limit: Int = 50) extends Connector[Data] {

  val dataStore: DataStore[Data] = new InMemLinearStore[Data]

  override def init(conf: PipeConfig): Unit = {}

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
