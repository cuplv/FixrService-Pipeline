package protopipes.builders

import protopipes.connectors.Connector
import protopipes.data.Identifiable
import protopipes.platforms.{ComputesMap, UnaryPlatform}
import protopipes.platforms.instances.MapperPlatform

/**
  * Created by edmundlam on 8/8/17.
  */


abstract class PlatformBuilder {

  def connector[Data <: Identifiable[Data]](name: String): Connector[Data]

  def mapperPlatform[Input <: Identifiable[Input],Output](): UnaryPlatform[Input,Output] with ComputesMap[Input,Output]

}
