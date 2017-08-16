package protopipes.connectors.instances

import protopipes.connectors.Connector
import protopipes.connectors.Connector.Id
import protopipes.connectors.Status.Status
import protopipes.data.Identity
import protopipes.platforms.Platform
import protopipes.store.DataStore
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/9/17.
  */

case class SequencedConnectors[Data](headConnector:Connector[Data], endConnector:Connector[Data]) extends Connector[Data] {

  headConnector.registerDownstreamConnector(endConnector)
  endConnector.registerUpstreamConnector(headConnector)

  override def init(conf: Config): Unit = {
     headConnector.init(conf)
     endConnector.init(conf)
  }

  override def terminate(): Unit = {
     headConnector.terminate()
     endConnector.terminate()
  }

  /*
  override def registerStore(datastore: DataStore[Data]): Unit = {
    super.registerStore(datastore)
    headConnector.registerStore(datastore)
    endConnector.registerStore(datastore)
  } */

  override def registerDownstreamConnector(connector: Connector[Data]): Unit = {
    super.registerDownstreamConnector(connector)
    endConnector.registerDownstreamConnector(connector)
  }

  override def registerPlatform(platform: Platform): Unit = {
    super.registerPlatform(platform)
    endConnector.registerPlatform(platform)
  }

  override def signalDown(): Unit = headConnector.signalDown()

  override def sendDown(data: Seq[Data]): Unit = headConnector.sendDown(data)

  override def sendDownModified(data: Seq[Data]): Unit = headConnector.sendDownModified(data)

  override def retrieveUp(): Seq[Data] = endConnector.retrieveUp()

  override def reportUp(status: Status, data: Seq[Data]): Unit = endConnector.reportUp(status, data)

  override def size(): Int = headConnector.size() + endConnector.size()

}
