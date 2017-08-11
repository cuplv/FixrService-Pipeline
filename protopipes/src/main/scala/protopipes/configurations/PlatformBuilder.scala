package protopipes.configurations

import protopipes.connectors.Connector
import protopipes.data.Identifiable
import protopipes.platforms.{BinaryPlatform, ComputesMap, ComputesPairwiseCompose, UnaryPlatform}
import protopipes.platforms.instances.MapperPlatform

/**
  * Created by edmundlam on 8/8/17.
  */


abstract class PlatformBuilder {

  def connector[Data <: Identifiable[Data]](name: String): Connector[Data]

  def mapperPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]](): UnaryPlatform[Input,Output] with ComputesMap[Input,Output]

  def pairwiseComposerPlatform[InputL <: Identifiable[InputL],InputR <: Identifiable[InputR],Output <: Identifiable[Output]]: BinaryPlatform[InputL,InputR,Output] with ComputesPairwiseCompose[InputL,InputR,Output]

}