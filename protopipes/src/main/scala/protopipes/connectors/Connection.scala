package protopipes.connectors

import protopipes.connectors.Connector.Id
import protopipes.connectors.Status.Status
import protopipes.platforms.Platform
import protopipes.store.DataStore
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */
abstract class Connection[Data] {

  var datastoreOpt: Option[DataStore[Data]] = None
  var platformOpt: Option[Platform] = None

  def init(conf: Config): Unit
  def terminate(): Unit

  // Registration methods

  def registerStore(datastore: DataStore[Data]): Unit = datastoreOpt = Some(datastore)
  def registerPlatform(platform: Platform): Unit = platformOpt = Some(platform)

  def getPlatform(): Platform = platformOpt match {
    case Some(platform) => platform
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  // upstream methods

  def sendDown(data: Seq[Data]): Unit

  // downstream methods

  def retrieveUp(): Seq[(Id,Data)]
  def reportUp(status: Status, ids: Seq[Id]): Unit

}
