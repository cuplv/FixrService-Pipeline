package protopipes.connectors

import protopipes.builders.PlatformBuilder
import protopipes.connectors.Connector.Id
import protopipes.connectors.Status.Status
import protopipes.connectors.instances.{CompositeConnector, PlatformStub}
import protopipes.data.Identity
import protopipes.platforms.Platform
import protopipes.store.DataStore
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */

object Event extends Enumeration {
  type Event = Value
  val Added, Modified = Value
}

object Status extends Enumeration {
  type Status = Value
  val Done, NotDone, Modified, Error = Value
}

object Connector {
  type Id = Long

  def incr(id: Id): Id = {
    if (id >= Long.MaxValue) 0L else id + 1
  }
}

abstract class Connector[Input] {

  // var datastoreOpt: Option[DataStore[Input]] = None
  //  var platformOpt: Option[Platform] = None

  var upstreamConnectorOpt: Option[Connector[Input]] = None
  var downstreamConnectorOpt: Option[Connector[Input]] = None

  def init(conf: Config): Unit
  def terminate(): Unit

  // def registerStore(datastore: DataStore[Input]): Unit = datastoreOpt = Some(datastore)
  def registerUpstreamConnector(connector: Connector[Input]): Unit = upstreamConnectorOpt = Some(connector)
  def registerDownstreamConnector(connector: Connector[Input]): Unit = downstreamConnectorOpt = Some(connector)
  def registerPlatform(platform: Platform): Unit = {
     // platformOpt = Some(platform)
     downstreamConnectorOpt = Some( PlatformStub(platform) )
  }

  /*
  def getPlatform(): Platform = platformOpt match {
    case Some(platform) => platform
    case None => {
      // TODO: Throw exception
      ???
    }
  } */

  def getUpstream(): Connector[Input] = upstreamConnectorOpt match {
    case Some(connector) => connector
    case None => {
      // TODO: throw exception
      ???
    }
  }

  def getDownstream(): Connector[Input] = downstreamConnectorOpt match {
    case Some(connector) => connector
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  /* Incoming methods, from upstream */

  // Signal downstream connector that new data are available
  def signalDown(): Unit = getDownstream().signalDown()

  // Send new data downstream
  def sendDown(data: Seq[Input]): Unit
  def sendDown(data: Input): Unit = sendDown(Seq(data))

  /* Incoming methods, from downstream */

  // Retrieve data from upstream
  def retrieveUp(): Seq[Input]

  // Send status report upstream
  def reportUp(status: Status, data: Seq[Input]): Unit
  def reportUp(status: Status, data: Input): Unit = reportUp(status, Seq(data))

  def size(): Int

  def +> (connector: Connector[Input]): Connector[Input] = CompositeConnector(this, connector)

}

trait Upstream[Data] {

  var downstreamConnectors: Seq[Connector[Data]] = Seq.empty[Connector[Data]]

  def registerConnector(connector: Connector[Data]): Unit = {
    downstreamConnectors = downstreamConnectors :+ connector
  }

  def transmitDownstream(data: Seq[Data]): Unit = {
    downstreamConnectors foreach { _.sendDown(data) }
  }

  def transmitDownstream(data: Data): Unit = transmitDownstream(Seq(data))

}

