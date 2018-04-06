package bigglue.connectors

import bigglue.configurations.{PipeConfig, PlatformBuilder}
import bigglue.connectors.Connector.Id
import bigglue.connectors.Status.Status
import bigglue.connectors.instances.{PlatformStub, SequencedConnectors}
import bigglue.data.BasicIdentity
import bigglue.platforms.Platform
import bigglue.store.DataStore
import com.typesafe.config.Config
import bigglue.exceptions.NotInitializedException

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

/**
  * This is what connects the pipes together. Being more specific, the connectors are what connect the input data maps
  * to [[Platform]]s. In the case of the example, this is what connects a together with AA, b together with BB, and c together with CC.
  * @tparam Input The type of data that's being sent down the pipeline from this connector.
  *              In the case of the example, it would be [[bigglue.data.I]][Int], as we're bringing in
  *              data from a in a:--AA-->b, b from b:--BB-->c, and c from c:-+CC+->d.
  */
abstract class Connector[Input] {

  var upstreamConnectorOpt: Option[Connector[Input]] = None
  var downstreamConnectorOpt: Option[Connector[Input]] = None

  def init(conf: PipeConfig): Unit
  def terminate(): Unit

  // def registerStore(datastore: DataStore[Input]): Unit = datastoreOpt = Some(datastore)
  def registerUpstreamConnector(connector: Connector[Input]): Unit = upstreamConnectorOpt = Some(connector)
  def registerDownstreamConnector(connector: Connector[Input]): Unit = downstreamConnectorOpt = Some(connector)

  /**
    * This connects the platform to the connector so the connector sends stuff to the platform.
    * As an implementation, this creates a dummy [[PlatformStub]] that's connected to the platform
    * and makes that it's downstream connector.
    * @param platform The platform that is being connected.
    */
  def registerPlatform(platform: Platform): Unit = {
     // platformOpt = Some(platform)
     downstreamConnectorOpt = Some( PlatformStub(platform) )
  }

  def persist(dataStore: DataStore[Input]): Unit = sendDown(dataStore.all())

  def getUpstream(): Connector[Input] = upstreamConnectorOpt match {
    case Some(connector) => connector
    case None => {
      throw new NotInitializedException("Connector", "getUpstream()", None)
    }
  }

  def getDownstream(): Connector[Input] = downstreamConnectorOpt match {
    case Some(connector) => connector
    case None => {
      throw new NotInitializedException("Connector", "getDownstream()", None)
    }
  }

  /* Incoming methods, from upstream */

  // Signal downstream connector that new data are available
  def signalDown(): Unit = downstreamConnectorOpt match {
    case Some(downstreamConnector) => downstreamConnector.signalDown()
    case None =>
  }

  // Send new data downstream
  def sendDown(data: Seq[Input]): Unit
  def sendDown(data: Input): Unit = sendDown(Seq(data))

  def sendDownModified(data: Seq[Input]): Unit
  def sendDownModified(data: Input): Unit = sendDownModified(Seq(data))

  /* Incoming methods, from downstream */

  // Retrieve data from upstream
  def retrieveUp(): Seq[Input]

  // Send status report upstream
  def reportUp(status: Status, data: Seq[Input]): Unit
  def reportUp(status: Status, data: Input): Unit = reportUp(status, Seq(data))

  def size(): Int

  def +> (connector: Connector[Input]): Connector[Input] = SequencedConnectors(this, connector)

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

  def transmitDownstreamModified(data: Seq[Data]): Unit = {
    downstreamConnectors foreach { _.sendDownModified(data) }
  }

  def transmitDownstreamModified(data: Data): Unit = transmitDownstreamModified(Seq(data))

}

