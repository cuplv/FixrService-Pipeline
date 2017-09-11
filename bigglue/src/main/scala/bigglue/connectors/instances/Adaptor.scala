package bigglue.connectors.instances

import bigglue.configurations.PipeConfig
import bigglue.connectors.Connector
import bigglue.connectors.Status.Status
import bigglue.exceptions.NotInitializedException
import bigglue.platforms.Platform

/**
  * Created by edmundlam on 8/30/17.
  */
abstract class Adaptor[Source,Target](connector: Connector[Source]) extends Connector[Target] {

  def toTarget(source: Source): Target

  def toSource(target: Target): Source

  override def init(conf: PipeConfig): Unit = {}

  override def terminate(): Unit = {}

  // override def registerUpstreamConnector(connector: Connector[Target]): Unit = ???
  // override def registerDownstreamConnector(connector: Connector[Target]): Unit = ???
  // override def registerPlatform(platform: Platform): Unit = ???

  // override def getUpstream(): Connector[Target] = ???

  // override def getDownstream(): Connector[Target] = ???

  // override def signalDown(): Unit = ???

  override def sendDown(data: Seq[Target]): Unit = connector.sendDown(data.map( toSource(_) ))

  override def sendDownModified(data: Seq[Target]): Unit = connector.sendDownModified(data.map( toSource(_) ))

  override def retrieveUp(): Seq[Target] = connector.retrieveUp().map( toTarget(_) )

  override def reportUp(status: Status, data: Seq[Target]): Unit = connector.reportUp(status, data.map( toSource(_) ))

  override def size(): Int = connector.size()

}


class IgnoreKeyAdaptor[Key,Source](connector: Connector[Source], defaultKey: Key) extends Adaptor[Source,(Key,Source)](connector) {

  override def toSource(target: (Key, Source)): Source = target._2

  override def toTarget(source: Source): (Key, Source) = (defaultKey, source)


}