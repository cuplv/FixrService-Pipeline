package bigglue.connectors.instances

import bigglue.connectors.Connector
import bigglue.connectors.Connector.Id
import bigglue.connectors.Status.Status
import bigglue.data.BasicIdentity
import bigglue.platforms.Platform
import com.typesafe.config.Config
import bigglue.configurations.PipeConfig
import bigglue.exceptions.CallNotAllowException

/**
  * Created by edmundlam on 8/9/17.
  */

/**
  * This is a wrapper for the platform as a connector.
  * This is only meant to be a downstream connector to the connectors in between the data store and the platform.
  * In the terms of the example, this is
  * @param platform The platform that is connected to the various connectors.
  * @tparam Data The type of the data of the input that the
  */
case class PlatformStub[Data](platform: Platform) extends Connector[Data] {

  override def init(conf: PipeConfig): Unit = { }
  override def terminate(): Unit = { }

  override def signalDown(): Unit = { platform.wake() }
  override def sendDown(data: Seq[Data]): Unit = {
    throw new CallNotAllowException("PlatformStub does not support \'sendDown\'", None)
  }

  override def sendDownModified(data: Seq[Data]): Unit = {
    throw new CallNotAllowException("PlatformStub does not support \'sendDownModified\'", None)
  }

  override def retrieveUp(): Seq[Data] = {
    throw new CallNotAllowException("PlatformStub does not support \'retrieveUp\'", None)
  }

  override def reportUp(status: Status, ids: Seq[Data]): Unit = {
    throw new CallNotAllowException("PlatformStub does not support \'reportUp\'", None)
  }

  override def size(): Int = 0

}
