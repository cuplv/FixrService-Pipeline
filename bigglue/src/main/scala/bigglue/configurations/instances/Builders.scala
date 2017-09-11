package bigglue.configurations.instances

import akka.actor.ActorSystem
import bigglue.configurations.PlatformBuilder
import bigglue.connectors.Connector
import bigglue.connectors.instances.{ActorIncrTrackerJobQueue, IncrTrackerJobQueue}
import bigglue.curators.{ErrorCurator, ProvenanceCurator, StandardErrorCurator, StandardProvenanceCurator}
import bigglue.data.{I, Identifiable}
import bigglue.platforms.instances.thinactors.{ThinActorMapperPlatform, ThinActorPairwiseComposerPlatform, ThinActorReducerPlatform, ThinActorUnaryPlatform}
import bigglue.platforms._

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
    val constructor = Class.forName("protopipes.platforms.instances.thinactors.ThinActorReducerPlatform")
      .getConstructors()(0)
    println(constructor)
    val args = Array[AnyRef]("testABC")
    val platform = constructor.newInstance( args:_* ).asInstanceOf[UnaryPlatform[Input,Output]]
    println(platform)
    platform
    // new ThinActorReducerPlatform[Input,Output]()
  }


}
