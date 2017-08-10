package protopipes.computations

import protopipes.builders.PlatformBuilder
import protopipes.data.Identifiable
import protopipes.pipes.{PartialMapperPipe, Pipe}
import protopipes.platforms.instances.MapperPlatform
import protopipes.platforms.{ComputesMap, Platform, UnaryPlatform}
import protopipes.store.DataStore
import com.typesafe.config.Config

import scala.util.Random

/**
  * Created by edmundlam on 8/8/17.
  */
abstract class Computation  {

  var platformOpt: Option[Platform] = None

  def init(config: Config, platform: Platform): Unit = platformOpt match {
    case None => {
      platformOpt = Some(platform)
    }
    case Some(_) => {
      // TODO: already init-id, log warning
    }
  }

  def run(): Unit = platformOpt match {
    case Some(platform) => platform.run()
    case None => {
      // TODO: Not init-ed. Throw exception
      ???
    }
  }

  def terminate(): Unit ={
    platformOpt match {
      case Some(platform) => platform.terminate()
      case None => {
        // TODO: not init-ed. Log warning.
      }
    }
  }

}

abstract class UnaryComputation[Input <: Identifiable[Input], Output](implicit builder: PlatformBuilder) extends Computation {

  var name: String = s"UnaryComputation-${Random.nextInt(99999)}"

  def name(newName: String): UnaryComputation[Input, Output] = { name = newName ; this }

  def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output], platform: UnaryPlatform[Input, Output]): Unit = {
    inputMap.registerConnector(platform.getUpstreamConnector())
    init(conf, platform)
  }

}

abstract class Mapper[Input <: Identifiable[Input], Output](implicit builder: PlatformBuilder) extends UnaryComputation[Input,Output] {

  def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
    val platform: UnaryPlatform[Input,Output] with ComputesMap[Input,Output] = builder.mapperPlatform[Input,Output]()
    platform.init(conf, inputMap, outputMap, builder)
    platform.setMapper(this)
    init(conf, inputMap, outputMap, platform)
  }

  def compute(input: Input): List[Output]

  def -->[End <: Identifiable[End]](p: Pipe[Output,End]): PartialMapperPipe[Input,Output,End] = PartialMapperPipe(this, p)

}