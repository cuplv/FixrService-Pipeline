package protopipes.configurations.instances

import akka.actor.ActorSystem
import protopipes.configurations.PlatformBuilder
import protopipes.connectors.Connector
import protopipes.connectors.instances.{ActorIncrTrackerJobQueue, IncrTrackerJobQueue}
import protopipes.curators.{ErrorCurator, ProvenanceCurator, StandardErrorCurator, StandardProvenanceCurator}
import protopipes.data.Identifiable
import protopipes.platforms.instances.thinactors.{ThinActorMapperPlatform, ThinActorPairwiseComposerPlatform, ThinActorReducerPlatform}
import protopipes.platforms._

/**
  * Created by edmundlam on 8/8/17.
  */


object ThinActorPlatformBuilder extends PlatformBuilder {

  override def connector[Data <: Identifiable[Data]](name: String): Connector[Data] = new IncrTrackerJobQueue[Data]

  override def provenanceCurator[Input <: Identifiable[Input], Output <: Identifiable[Output]]: ProvenanceCurator[Input, Output] = {
    new StandardProvenanceCurator[Input,Output]
  }

  override def errorCurator[Input <: Identifiable[Input]]: ErrorCurator[Input] = {
    new StandardErrorCurator[Input]
  }

  override def mapperPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]](): UnaryPlatform[Input,Output] = new ThinActorMapperPlatform[Input,Output]

  override def pairwiseComposerPlatform[InputL <: Identifiable[InputL],InputR <: Identifiable[InputR],Output <: Identifiable[Output]]: BinaryPlatform[InputL,InputR,Output]
        = {
    new ThinActorPairwiseComposerPlatform[InputL,InputR,Output]()
  }

  override def reducerPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]](): UnaryPlatform[Input, Output]
        = {
    new ThinActorReducerPlatform[Input,Output]()
  }


}
