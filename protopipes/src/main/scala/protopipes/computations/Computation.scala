package protopipes.computations

import protopipes.configurations.PlatformBuilder
import protopipes.data.Identifiable
import protopipes.pipes.{PartialComposerPipe, PartialMapperPipe, Pipe}
import protopipes.platforms.instances.MapperPlatform
import protopipes.platforms._
import protopipes.store.DataStore
import com.typesafe.config.Config
import protopipes.data

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

abstract class Mapper[Input <: Identifiable[Input], Output <: Identifiable[Output]](implicit builder: PlatformBuilder) extends UnaryComputation[Input,Output] {

  def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
    val platform: UnaryPlatform[Input,Output] with ComputesMap[Input,Output] = builder.mapperPlatform[Input,Output]()
    platform.init(conf, inputMap, outputMap, builder)
    platform.setMapper(this)
    init(conf, inputMap, outputMap, platform)
  }

  def compute(input: Input): List[Output]

  def -->[End <: Identifiable[End]](p: Pipe[Output,End]): PartialMapperPipe[Input,Output,End] = PartialMapperPipe(this, p)

}

abstract class BinaryComputation[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output](implicit builder: PlatformBuilder) extends Computation {

  var name: String = s"BinaryComputation-${Random.nextInt(99999)}"

  def name(newName: String): BinaryComputation[InputL,InputR, Output] = { name = newName ; this }

  def init(conf: Config, inputMapL: DataStore[InputL], inputMapR: DataStore[InputR], outputMap: DataStore[Output], platform: BinaryPlatform[InputL,InputR,Output]): Unit = {
     inputMapL.registerConnector(platform.getUpstreamLConnector())
     inputMapR.registerConnector(platform.getUpstreamRConnector())
     init(conf, platform)
  }

}

abstract class PairwiseComposer[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]](implicit builder: PlatformBuilder) extends BinaryComputation[InputL,InputR,Output] {

  def init(conf: Config, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output]): Unit = {
     val platform: BinaryPlatform[InputL,InputR,Output] with ComputesPairwiseCompose[InputL,InputR,Output] = builder.pairwiseComposerPlatform[InputL,InputR,Output]
     platform.init(conf, inputLMap, inputRMap, outputMap, builder)
     platform.setPairwiseComposer(this)
     init(conf, inputLMap, inputRMap, outputMap, platform)
  }

  def filter(inputL: InputL, inputR: InputR): Boolean

  def compose(inputL: InputL, inputR: InputR): Output

  def *->[End <: Identifiable[End]](output: Pipe[Output,End]): PartialComposerPipe[InputL,InputR,Output,End] = {
    PartialComposerPipe(this, output)
  }

}

class CartesianProduct[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR]](implicit builder: PlatformBuilder) extends PairwiseComposer[InputL,InputR,protopipes.data.Pair[InputL,InputR]] {

  override def filter(inputL: InputL, inputR: InputR): Boolean = true

  override def compose(inputL: InputL, inputR: InputR): data.Pair[InputL, InputR] = protopipes.data.Pair(inputL,inputR)
  
}

