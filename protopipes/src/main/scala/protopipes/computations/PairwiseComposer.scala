package protopipes.computations

import protopipes.configurations.{ConfOpt, PipeConfig, PlatformBuilder}
import protopipes.data.{Identifiable, BasicIdentity}
import protopipes.pipes.{PartialComposerPipe, PartialMapperPipe, PartialReducerPipe, Pipe}
import protopipes.platforms.instances.MapperPlatform
import protopipes.platforms._
import protopipes.store.{DataMap, DataStore}
import com.typesafe.config.Config
import protopipes.connectors.Status
import protopipes.data

import scala.util.Random

/**
  * Created by edmundlam on 8/8/17.
  */

abstract class PairwiseComposer[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]]
     extends BinaryComputation[InputL,InputR,Output] {

  def withConfig(newConfigOption: ConfOpt): PairwiseComposer[InputL, InputR, Output] = {
    configOption = newConfigOption
    this
  }

  def init(conf: Config, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output]): Unit = {
     val rconf = PipeConfig.resolveOptions(conf, configOption)
     val builder = PlatformBuilder.load(rconf)
     val platform: BinaryPlatform[InputL,InputR,Output] = builder.pairwiseComposerPlatform[InputL,InputR,Output]
     platform.init(rconf, inputLMap, inputRMap, outputMap, builder)
     // platform.setPairwiseComposer(this)
     platform.setComputation(this)
     init(rconf, inputLMap, inputRMap, outputMap, platform)
  }

  def filter(inputL: InputL, inputR: InputR): Boolean

  def compose(inputL: InputL, inputR: InputR): Output

  def tryFilterAndCompose(pair: protopipes.data.Pair[InputL,InputR]): Option[Output] = {
    val platform = getBinaryPlatform()
    try {
      val output = if(filter(pair.left, pair.right)) {
        val output = platform.getVersionCurator().stampVersion( compose(pair.left, pair.right) )
        platform.getOutputMap().put(output)
        platform.getProvenanceCurator().reportProvenance(pair, output)
        Some(output)
      } else None
      platform.getPairConnector().reportUp(Status.Done, pair)
      output
    } catch {
      case ex: Exception => {
        platform.getPairErrorCurator().reportError(pair, ex,
             Some(s"PairwiseComposer operations \'filter\' and \'compose\' failed on inputs ${pair.left} and ${pair.right}."))
        None
      }
    }
  }

  override def run(): Unit = {
    val platform = getBinaryPlatform()
    val inputs = platform.getInputs()
    inputs._3 foreach {
      tryFilterAndCompose(_)
    }
    platform.getUpstreamLConnector().reportUp(Status.Done,inputs._1)
    platform.getUpstreamRConnector().reportUp(Status.Done,inputs._2)
  }

  def *->[End <: Identifiable[End]](output: Pipe[Output,End]): PartialComposerPipe[InputL,InputR,Output,End] = {
    PartialComposerPipe(this, output)
  }

}

class CartesianProduct[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR]] extends PairwiseComposer[InputL,InputR,protopipes.data.Pair[InputL,InputR]] {

  override def filter(inputL: InputL, inputR: InputR): Boolean = true

  override def compose(inputL: InputL, inputR: InputR): data.Pair[InputL, InputR] = protopipes.data.Pair(inputL,inputR)
  
}

