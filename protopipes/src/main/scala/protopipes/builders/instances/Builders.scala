package protopipes.builders.instances

import akka.actor.ActorSystem
import protopipes.builders.PlatformBuilder
import protopipes.connectors.Connector
import protopipes.connectors.instances.{ActorIncrTrackerJobQueue, IncrTrackerJobQueue}
import protopipes.data.Identifiable
import protopipes.platforms.{ComputesMap, UnaryPlatform}
import protopipes.platforms.instances.{MapperPlatform, ThinActorMapperPlatform}

/**
  * Created by edmundlam on 8/8/17.
  */


object ThinActorPlatformBuilder extends PlatformBuilder {

  def connector[Data <: Identifiable[Data]](name: String): Connector[Data] = new IncrTrackerJobQueue[Data]

  def mapperPlatform[Input <: Identifiable[Input],Output](): UnaryPlatform[Input,Output] with ComputesMap[Input,Output] = new ThinActorMapperPlatform[Input,Output]

}
