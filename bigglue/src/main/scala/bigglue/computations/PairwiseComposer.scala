package bigglue.computations

import bigglue.configurations.{ConfOpt, PipeConfig, PlatformBuilder}
import bigglue.data.{BasicIdentity, Identifiable}
import bigglue.pipes.{PartialComposerPipe, PartialMapperPipe, PartialReducerPipe, Pipe}
import bigglue.platforms.instances.MapperPlatform
import bigglue.platforms._
import bigglue.store.{DataMap, DataStore}
import com.typesafe.config.Config
import bigglue.connectors.Status
import bigglue.data
import bigglue.exceptions.UserComputationException

import scala.util.Random

/**
  * Created by edmundlam on 8/8/17.
  */
/*
case class PairwiseCompose[InputL,InputR,Output](filter: InputL => InputR => Boolean, compose: InputL => InputR => Output)

object PairwiseCompose {
  def cartesian[InputL,InputR]: PairwiseCompose[InputL,InputR,protopipes.data.Pair[InputL,InputR]] = {
    PairwiseCompose( _ => _ => true , inputL => inputR => protopipes.data.Pair(inputL,inputR) )
  }
} */

class PairwiseComposer[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]]
   (filter: InputL => InputR => Boolean, compose: InputL => InputR => Output) extends BinaryComputation[InputL,InputR,Output] {

  def withConfig(newConfigOption: ConfOpt): PairwiseComposer[InputL, InputR, Output] = {
    configOption = newConfigOption
    this
  }

  def init(conf: PipeConfig, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output]): Unit = {
     val rconf = PipeConfig.resolveOptions(conf, configOption)
     val builder = constructBuilder(rconf) // PlatformBuilder.load(rconf)
     val platform: BinaryPlatform[InputL,InputR,Output] = builder.pairwiseComposerPlatform[InputL,InputR,Output]
     platform.init(rconf, inputLMap, inputRMap, outputMap, builder)
     // platform.setPairwiseComposer(this)
     platform.setComputation(this)
     init(rconf, inputLMap, inputRMap, outputMap, platform)
  }

  // def filter(inputL: InputL, inputR: InputR): Boolean

  // def compose(inputL: InputL, inputR: InputR): Output

  def tryFilterAndCompose(pair: bigglue.data.Pair[InputL,InputR]): Option[Output] = {
    val platform = getBinaryPlatform()
    try {
      val output = if(filter(pair.left)(pair.right)) {
        val output = platform.getVersionCurator().stampVersion( compose(pair.left)(pair.right) )
        platform.getOutputMap().put(output)
        platform.getProvenanceCurator().reportProvenance(pair, output)
        Some(output)
      } else None
      platform.getPairConnector().reportUp(Status.Done, pair)
      output
    } catch {
      case ex: Exception => {
        val pex = new UserComputationException(s"PairwiseComposer \'filter\' or \'compose\'", Some(ex))
        platform.getPairErrorCurator().reportError(pair, pex,
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

class CartesianProduct[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR]] extends
  PairwiseComposer[InputL,InputR,bigglue.data.Pair[InputL,InputR]](
    filter = _ => _ => true,
    compose = inputL => inputR => bigglue.data.Pair(inputL,inputR)
  )

/*
{
  // override def filter(inputL: InputL, inputR: InputR): Boolean = true
  // override def compose(inputL: InputL, inputR: InputR): data.Pair[InputL, InputR] = protopipes.data.Pair(inputL,inputR)
} */

