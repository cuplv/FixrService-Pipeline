package protopipes.connectors.instances

import com.typesafe.config.Config
import protopipes.connectors.Connector
import protopipes.connectors.Status.Status
import protopipes.platforms.Platform

/**
  * Created by edmundlam on 8/13/17.
  */
class ParallelConnectors[Data](leftConnector: Connector[Data], rightConnector: Connector[Data]) extends Connector[Data] {

  override def init(conf: Config): Unit = {
     leftConnector.init(conf)
     rightConnector.init(conf)
  }

  override def terminate(): Unit = {
     leftConnector.terminate()
     rightConnector.terminate()
  }

  override def registerDownstreamConnector(connector: Connector[Data]): Unit = {
    super.registerDownstreamConnector(connector)
    leftConnector.registerDownstreamConnector(connector)
    rightConnector.registerDownstreamConnector(connector)
  }

  override def registerPlatform(platform: Platform): Unit = {
    super.registerPlatform(platform)
    leftConnector.registerPlatform(platform)
    rightConnector.registerPlatform(platform)
  }

  override def signalDown(): Unit = {
    leftConnector.signalDown()
    rightConnector.signalDown()
  }

  override def sendDown(data: Seq[Data]): Unit = {
    leftConnector.sendDown(data)
    rightConnector.sendDown(data)
  }

  override def sendDownModified(data: Seq[Data]): Unit = {
    leftConnector.sendDownModified(data)
    rightConnector.sendDownModified(data)
  }

  override def retrieveUp(): Seq[Data] = leftConnector.retrieveUp() ++ rightConnector.retrieveUp()

  override def reportUp(status: Status, data: Seq[Data]): Unit = {
    leftConnector.reportUp(status, data)
    rightConnector.reportUp(status, data)
  }

  override def size(): Int = leftConnector.size() + rightConnector.size()

}
