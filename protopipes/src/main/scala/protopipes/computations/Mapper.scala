package protopipes.computations

import com.typesafe.config.Config
import protopipes.configurations.{ConfigOption, DefaultOption, PipeConfig, PlatformBuilder}
import protopipes.connectors.Status
import protopipes.data.Identifiable
import protopipes.pipes.{PartialMapperPipe, Pipe}
import protopipes.platforms.UnaryPlatform
import protopipes.store.DataStore

/**
  * Created by edmundlam on 8/14/17.
  */

abstract class Mapper[Input <: Identifiable[Input], Output <: Identifiable[Output]] extends UnaryComputation[Input,Output] {

  def withConfig(newConfigOption: ConfigOption): Mapper[Input,Output] = {
    configOption = newConfigOption
    this
  }

  def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
    val rconf = PipeConfig.resolveOptions(conf, configOption)
    val builder = PlatformBuilder.load(rconf)
    val platform: UnaryPlatform[Input,Output] = builder.mapperPlatform[Input,Output]()
    platform.init(rconf, inputMap, outputMap, builder)
    // platform.setMapper(this)
    platform.setComputation(this)
    init(rconf, inputMap, outputMap, platform)
  }

  def compute(input: Input): List[Output]

  def tryCompute(input: Input): Option[List[Output]] = {
    val platform = getUnaryPlatform()
    try {
      val outputs = compute(input).map(
        output => {
          platform.getOutputMap().put(output)
          output
        }
      )
      platform.getUpstreamConnector().reportUp(Status.Done, input)
      platform.getProvenanceCurator().reportProvenance(input, outputs)
      Some(outputs)
    } catch {
      case ex: Exception => {
        // Compute exception occurred, log this in error store
        platform.getErrorCurator().reportError(input, ex, Some(s"Mapper \'compute\' operation failed on input $input."))
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
