package protopipes.computations

import com.typesafe.config.Config
import protopipes.configurations.{ConfOpt, DefaultOpt, PipeConfig, PlatformBuilder}
import protopipes.connectors.Status
import protopipes.data.Identifiable
import protopipes.exceptions.UserComputationException
import protopipes.pipes.{PartialMapperPipe, Pipe}
import protopipes.platforms.UnaryPlatform
import protopipes.store.DataStore

/**
  * Created by edmundlam on 8/14/17.
  */

class Mapper[Input <: Identifiable[Input], Output <: Identifiable[Output]]
        (op: Input => List[Output]) extends UnaryComputation[Input,Output] {

  def getOp = op

  def withConfig(newConfigOption: ConfOpt): Mapper[Input,Output] = {
    configOption = newConfigOption
    this
  }

  def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
    val rconf = PipeConfig.resolveOptions(conf, configOption)
    val builder = constructBuilder(rconf) // PlatformBuilder.load(rconf)
    val platform: UnaryPlatform[Input,Output] = builder.mapperPlatform[Input,Output]()
    platform.setComputation(this)
    platform.init(rconf, inputMap, outputMap, builder)
    // platform.setMapper(this)
    init(rconf, inputMap, outputMap, platform)
  }

  // def compute(input: Input): List[Output]

  def tryCompute(input: Input): Option[List[Output]] = {
    val platform = getUnaryPlatform()
    try {
      val outputs = op(input).map(
        output => {
          val voutput = platform.getVersionCurator().stampVersion(output)
          platform.getOutputMap().put(voutput)
          voutput
        }
      )
      platform.getUpstreamConnector().reportUp(Status.Done, input)
      platform.getProvenanceCurator().reportProvenance(input, outputs)
      Some(outputs)
    } catch {
      case ex: Exception => {
        // Compute exception occurred, log this in error store
        val pex = new UserComputationException("Mapper \'compute\'", Some(ex))
        platform.getErrorCurator().reportError(input, pex, Some(s"Mapper \'compute\' operation failed on input $input."))
        None
      }
    }
  }

  override def run(): Unit = {
    val platform = getUnaryPlatform()
    platform.getInputs() foreach {
      input => tryCompute(input)
    }
  }

  def -->[End <: Identifiable[End]](p: Pipe[Output,End]): PartialMapperPipe[Input,Output,End] = PartialMapperPipe(this, p)

}
