package protopipes.computations

import com.typesafe.config.Config
import protopipes.configurations.{ConfOpt, PipeConfig, PlatformBuilder}
import protopipes.connectors.Status
import protopipes.data.{BasicIdentity, Identifiable, Identity}
import protopipes.exceptions.{IncompatiblePipelineSegmentException, UserComputationException}
import protopipes.pipes.{PartialReducerPipe, Pipe}
import protopipes.platforms.UnaryPlatform
import protopipes.store.{DataMap, DataStore}

/**
  * Created by edmundlam on 8/14/17.
  */


abstract class Reducer[Input <: Identifiable[Input], Output <: Identifiable[Output]] extends UnaryComputation[Input,Output] {

  def withConfig(newConfigOption: ConfOpt): Reducer[Input,Output] = {
    configOption = newConfigOption
    this
  }

  override def checkOutput(outputMap: DataStore[Output]): Unit = {
    if (!outputMap.isInstanceOf[DataMap[_,_]]) {
       val context = s"Output map for Reducer \'$name\' needs to be a DataMap, but ${outputMap.name} is a ${outputMap.getClass.getName}"
       throw new IncompatiblePipelineSegmentException(context, None)
    }
  }

  def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
    val rconf = PipeConfig.resolveOptions(conf, configOption)
    val builder = PlatformBuilder.load(rconf)
    val platform: UnaryPlatform[Input, Output] = builder.reducerPlatform[Input,Output]()
    platform.init(rconf, inputMap, outputMap, builder)
    // platform.setReducer(this)
    platform.setComputation(this)
    init(rconf, inputMap, outputMap, platform)
  }

  def groupBy(input: Input): Identity[Output]

  def fold(input: Input, output: Output): Output

  def zero(): Output

  def tryGroupBy(input: Input): Option[Identity[Output]] = {
    val platform = getUnaryPlatform()
    try {
      val outputId = groupBy(input)
      platform.getUpstreamConnector().reportUp(Status.Done, input)
      Some(outputId)
    } catch {
      case ex: Exception => {
        val pex = new UserComputationException(s"Reducer \'groupBy\'", Some(ex))
        platform.getErrorCurator().reportError(input, pex, Some(s"Reducer \'groupBy\' operation failed on input $input."))
        None
      }
    }
  }

  def tryFold(input: Input, output: Output): Option[Output] = {
    val platform = getUnaryPlatform()
    try {
      val newOutput = platform.getVersionCurator().stampVersion(fold(input, output))
      platform.getUpstreamConnector.reportUp(Status.Done, input)
      platform.getProvenanceCurator.reportProvenance(input, output)
      Some(newOutput)
    } catch {
      case ex: Exception => {
        val pex = new UserComputationException(s"Reducer \'fold\'", Some(ex))
        platform.getErrorCurator.reportError(input, ex, Some(s"Reducer \'fold\' operation failed on inputs $input and $output."))
        None
      }
    }
  }

  def tryZero(): Option[Output] = {
    val platform = getUnaryPlatform()
    try {
      Some(zero())
    } catch {
      case ex: Exception => {
        // TODO: Log this somewhere
        // val pex = new UserComputationException(s"Reducer \'zero\'", Some(ex))
        // platform.getErrorCurator.reportError(input, ex, Some(s"Reducer \'fold\' operation failed on inputs $input and $output."))
        None
      }
    }
  }

  override def run(): Unit = {
    val platform = getUnaryPlatform()
    var mergals = Map.empty[Identity[Output],Seq[Input]]
    platform.getInputs() foreach {
      input => {
        tryGroupBy(input) match {
          case Some(outputId) => mergals = mergals + (outputId -> (mergals.getOrElse (outputId, Seq.empty[Input]) :+ input) )
          case None => // error already logged by tryGroupBy. Ignoring here.
        }
      }
    }
    val outputMap = platform.getOutputMap().asInstanceOf[DataMap[Identity[Output],Output]]
    mergals.toList foreach {
      pair => {
        tryZero() match {
          case Some(zero) => {
            val oid = platform.getVersionCurator().stampVersion(pair._1)
            val output = outputMap.getOrElse(  oid , zero)
            val newOutput = pair._2.foldRight(output) {
              (i, o) => tryFold(i, o) match {
                case Some(no) => no
                case None => o
              }
            }
            val vNewOutput = platform.getVersionCurator().stampVersion(newOutput)
            if (output != vNewOutput) {
              outputMap.put(vNewOutput)
            }
          }
          case None => {
            // Zero operation failed. Error already logged by tryZero. Ignoring here.
          }
        }
      }
    }
  }

  def +->[End <: Identifiable[End]](p: Pipe[Output,End]): PartialReducerPipe[Input,Output,End] = PartialReducerPipe(this, p)

}