package protopipes.configurations

import protopipes.connectors.Connector
import protopipes.curators.{ErrorCurator, ProvenanceCurator}
import protopipes.data.Identifiable
import protopipes.platforms._
import protopipes.platforms.instances.MapperPlatform

/**
  * Created by edmundlam on 8/8/17.
  */


abstract class PlatformBuilder {

  def connector[Data <: Identifiable[Data]](name: String): Connector[Data]

  def provenanceCurator[Input <: Identifiable[Input], Output <: Identifiable[Output]]: ProvenanceCurator[Input,Output]

  def errorCurator[Input <: Identifiable[Input]]: ErrorCurator[Input]

  def mapperPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]](): UnaryPlatform[Input,Output] // with ComputesMap[Input,Output]

  def reducerPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]](): UnaryPlatform[Input,Output] // with ComputesReduce[Input,Output]

  def pairwiseComposerPlatform[InputL <: Identifiable[InputL],InputR <: Identifiable[InputR],Output <: Identifiable[Output]]: BinaryPlatform[InputL,InputR,Output] // with ComputesPairwiseCompose[InputL,InputR,Output]

}