package bigglue.computations

import com.typesafe.config.Config
import bigglue.configurations.{ConfOpt, PipeConfig, PlatformBuilder}
import bigglue.connectors.Status
import bigglue.data.{BasicIdentity, Identifiable, Identity}
import bigglue.exceptions.{IncompatiblePipelineSegmentException, UserComputationException}
import bigglue.pipes.{PartialReducerPipe, Pipe}
import bigglue.platforms.UnaryPlatform
import bigglue.store.{DataMap, DataStore}

/**
  * Created by edmundlam on 8/14/17.
  */

// case class Reduce[Input,Output](groupBy: Input => Identity[Output], fold: Input => Output => Output, zero: Output)
/**
  * The Reducer Computation, which is a subclass of [[UnaryComputation]]
  * In short, this takes an input, groups it together with an output, and then updates the output with the input.
  * @param groupBy This is the function that has you group an input with an output. You choose the output in this case
  *                by it's [[Identity]]. In the case of the example, this would be the i => BasicIdentity(i.author) function.
  * @param fold This is the function that has you update the output with the new input. In the case of the example, this would
  *             be the i => o => GitCommitGroups(i.author, o.gitCommitInfos+1) function, where i is the new input and o is the current output.
  * @param zero This is the default output. This is given when the groupBy function doesn't lead to an actual output, making this
  *             the output that all outputs began as. Within the example, this would be GitCommitGroups("", 0).
  * @tparam Input The type of the data that needs to be computed. In this case, this is [[bigglue.examples.GitCommitInfo]]
  *               This needs to be an [[Identifiable]] type.
  * @tparam Output The type of the data that needs to be computed. In this case, this is [[bigglue.examples.GitCommitGroups]]
  *               This needs to be an [[Identifiable]] type.
  */
class Reducer[Input <: Identifiable[Input], Output <: Identifiable[Output]]
   (groupBy: Input => List[Identity[Output]]
   // ( groupBy: Input => Identity[Output]
   , fold: Input => Output => Output
   , zero: Output) extends UnaryComputation[Input,Output] {

  def withConfig(newConfigOption: ConfOpt): Reducer[Input,Output] = {
    configOption = newConfigOption
    this
  }

  /**
    * This allows us to type check the Reducer step;
    * In order for it to work, the output needs to be a DataMap, so we need to make sure that it is before continuing.
    * @param outputMap The Output Store to Check
    */
  override def checkOutput(outputMap: DataStore[Output]): Unit = {
    if (!outputMap.isInstanceOf[DataMap[_,_]]) {
       val context = s"Output map for Reducer \'$name\' needs to be a DataMap, but ${outputMap.name} is a ${outputMap.getClass.getName}"
       throw new IncompatiblePipelineSegmentException(context, None)
    }
  }

  /**
    * This initializes the Reducer Computation.
    * It creates a [[UnaryPlatform]] with [[PlatformBuilder.reducerPlatform]], then sets itself as the platform's computation,
    * and then initializes the platform with [[UnaryPlatform.init]].
    * @param conf The configuration file to build from.
    *             Note: If there is a GitCommitInfo+->GitAuthors section in the bigglue section of the configuration file,
    *             it will overwrite part of the configuration file with the values within the step's section.
    * @param inputMap The [[DataStore]] that data is being sent in from.
    *                 In the example, this would be commitInfoMap, which is implemented with a [[bigglue.store.instances.solr.SolrDataMap]].
    * @param outputMap The [[DataStore]] that data is being sent to and updated from.
    *                  In the example, this would be authorMap, which is implemented with a [[bigglue.store.instances.solr.SolrDataMap]].
    */
  def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
    val stepNm = s"${inputMap.displayName()}+->${outputMap.displayName()}"
    val rconf = toStep(PipeConfig.resolveOptions(conf, configOption), stepNm)
    val builder = constructBuilder(rconf) // PlatformBuilder.load(rconf)
    val platform: UnaryPlatform[Input, Output] = builder.reducerPlatform[Input,Output]()
    platform.setComputation(this)
    platform.init(rconf, inputMap, outputMap, builder)
    // platform.setReducer(this)
    init(rconf, inputMap, outputMap, platform)
  }

  // def groupBy(input: Input): Identity[Output]

  // def fold(input: Input, output: Output): Output

  // def zero(): Output

  /**
    * Attempts to call [[groupBy]] to get an Identity to get stuff to group by.
    * If that fails, then it just returns None.
    * @param input The input document to find an output Identity for.
    * @return The output identity to change; None if none.
    */
  def tryGroupBy(input: Input): List[Identity[Output]] = {
    val platform = getUnaryPlatform()
    try {
      val outputId = groupBy(input)
      platform.getUpstreamConnector().reportUp(Status.Done, input)
      outputId
    } catch {
      case ex: Exception => {
        val pex = new UserComputationException(s"Reducer \'groupBy\'", Some(ex))
        platform.getErrorCurator().reportError(input, pex, Some(s"Reducer \'groupBy\' operation failed on input $input."))
        List()
      }
    }
  }

  /**
    * Attempts to call [[fold]] to get a new output for the Output Data Store
    * If it fails, just returns None.
    * @param input The input document to update the output.
    * @param output The output document that needs to be updated.
    * @return A new output document based on input and output. Returns None if failed.
    */
  def tryFold(input: Input, output: Output): Option[Output] = {
    val platform = getUnaryPlatform()
    try {
      val newOutput = platform.getVersionCurator().stampVersion(fold(input)(output))
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

  /**
    * Attempts to call [[zero]] to get a starting output set.
    * @return The default output. In the case of the example, this would be Some(Counter(0)). Returns None if failed to get a default output.
    */
  def tryZero(): Option[Output] = {
    val platform = getUnaryPlatform()
    try {
      Some(zero)
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
        tryGroupBy(input) foreach {
          outputId => mergals = mergals + (outputId -> (mergals.getOrElse (outputId, Seq.empty[Input]) :+ input) )
          /*case Some(outputId) => mergals = mergals + (outputId -> (mergals.getOrElse (outputId, Seq.empty[Input]) :+ input) )
          case None => // error already logged by tryGroupBy. Ignoring here.*/
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

  /**
    * This is part of the series of calls that make the pipeline. This is called within the FindAuthor()+->authorMap step.
    * This links the reducer to the rest of the pipeline, which in this case is [[bigglue.pipes.Implicits.DataNode]](authorMap).
    * @param p The part of the pipeline that follows the reducer computation. In the example, this is simply authorMap.
    * @tparam End The type of the final data store within the pipeline. Within the example, this would be
    *             [[bigglue.examples.GitCommitGroups]].
    * @return This returns a section of the pipe where the reducer is linked together with the pipe that starts with the
    *         output data store.
    */
  def +->[End <: Identifiable[End]](p: Pipe[Output,End]): PartialReducerPipe[Input,Output,End] = PartialReducerPipe(this, p)

}