package protopipes.configurations.instances

import akka.actor.ActorSystem
import protopipes.configurations.PlatformBuilder
import protopipes.connectors.Connector
import protopipes.connectors.instances.{ActorIncrTrackerJobQueue, IncrTrackerJobQueue}
import protopipes.data.Identifiable
import protopipes.platforms.{BinaryPlatform, ComputesMap, ComputesPairwiseCompose, UnaryPlatform}
import protopipes.platforms.instances.{MapperPlatform, ThinActorMapperPlatform, ThinActorPairwiseComposerPlatform}

/**
  * Created by edmundlam on 8/8/17.
  */


object ThinActorPlatformBuilder extends PlatformBuilder {

  override def connector[Data <: Identifiable[Data]](name: String): Connector[Data] = new IncrTrackerJobQueue[Data]

  override def mapperPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]](): UnaryPlatform[Input,Output] with ComputesMap[Input,Output] = new ThinActorMapperPlatform[Input,Output]

  override def pairwiseComposerPlatform[InputL <: Identifiable[InputL],InputR <: Identifiable[InputR],Output <: Identifiable[Output]]: BinaryPlatform[InputL,InputR,Output]
       with ComputesPairwiseCompose[InputL,InputR,Output] = {
       new ThinActorPairwiseComposerPlatform[InputL,InputR,Output]()
  }


}
