package protopipes.connectors.instances

import protopipes.connectors.Connector
import protopipes.connectors.Connector.Id
import protopipes.connectors.Status.Status
import protopipes.data.Identity
import protopipes.platforms.Platform
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/9/17.
  */
case class PlatformStub[Data](platform: Platform) extends Connector[Data] {

  override def init(conf: Config): Unit = { }
  override def terminate(): Unit = { }

  override def signalDown(): Unit = { platform.wake() }
  override def sendDown(data: Seq[Data]): Unit = {
    // TODO Throw exception: Not allowed
    ???
  }

  override def retrieveUp(): Seq[Data] = {
    // TODO Throw exception: Not allowed
    ???
  }

  override def reportUp(status: Status, ids: Seq[Data]): Unit = {
    // TODO Throw exception: Not allowed
    ???
  }

  override def size(): Int = 0

}
