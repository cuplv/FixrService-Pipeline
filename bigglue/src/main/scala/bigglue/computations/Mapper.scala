package bigglue.computations

import com.typesafe.config.Config
import bigglue.configurations.{ConfOpt, DefaultOpt, PipeConfig, PlatformBuilder}
import bigglue.connectors.Status
import bigglue.data.Identifiable
import bigglue.exceptions.UserComputationException
import bigglue.pipes.{PartialMapperPipe, Pipe}
import bigglue.platforms.UnaryPlatform
import bigglue.store.DataStore

/**
  * Created by edmundlam on 8/14/17.
  */

/**
  * The Mapper Computation, which is a subclass of [[UnaryComputation]].
  * In short, this takes a input, the computation turns that into a list of outputs.
  * In the example, this would be either Clone() or CommitExtraction().
  * @param op The computation to be run by the computation. For both, this is this section that's in between the parentheses.
  * @tparam Input The type of the data that needs to be computed.
  *               Within the example, this is [[bigglue.examples.GitID]] for GitClone(), and [[bigglue.examples.GitRepo]] for GitCommitExtraction().
  *               This needs to be an [[Identifiable]] type.
  * @tparam Output The type of the data that ends up being computed.
  *                Within the example, this is [[bigglue.examples.GitRepo]] for GitClone(), and [[bigglue.examples.GitCommitInfo]] for GitCommitExtraction().
  *                This also needs to be an [[Identifiable]] type.
  */
class Mapper[Input <: Identifiable[Input], Output <: Identifiable[Output]]
        (op: Input => List[Output]) extends UnaryComputation[Input,Output] {

  def getOp = op

  def withConfig(newConfigOption: ConfOpt): Mapper[Input,Output] = {
    configOption = newConfigOption
    this
  }

  /**
    * This initializes the mapper computation.
    * It creates a [[UnaryPlatform]] with [[PlatformBuilder.mapperPlatform]], then sets itself as the platform's computation,
    * and then initializes the platform with [[UnaryPlatform.init]].
    * @param conf The configuration file to build from.
    *             Note: If there is a GitIDs-->GitRepos section or GitRepos-->GitCommitInfo section in the bigglue section of the configuration file,
    *             it will overwrite part of the configuration file with the values within the step's section.
    *             You can see this in effect with GitIDs-->GitRepos, which changes the connector from an [[bigglue.connectors.instances.IncrTrackerJobQueue]] to a [[bigglue.connectors.instances.JobQueue]].
    * @param inputMap The [[DataStore]] that data is being sent in from.
    *                 Within the example, for gitID:--Clone()-->clonedMap, this would be gitID. Likewise, for clonedMap:--CommitExtraction()-->commitInfoMap, this would be clonedMap.
    *                 Both of these Data Stores are [[bigglue.store.instances.solr.SolrDataMap]] within the example.
    * @param outputMap The [[DataStore]] that data is being sent to after computation.
    *                  Within the example, for gitID:--Clone()-->clonedMap, this would be clonedMap. Likewise, for clonedMap:--CommitExtraction()-->commitInfoMap, this would be commitInfoMap.
    *                  Both of these Data Stores are [[bigglue.store.instances.solr.SolrDataMap]] within the example.
    */
  def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
    val stepNm = s"${inputMap.displayName()}-->${outputMap.displayName()}"
    val rconf = toStep(PipeConfig.resolveOptions(conf, configOption), stepNm)
    val builder = constructBuilder(rconf) // PlatformBuilder.load(rconf)
    val platform: UnaryPlatform[Input,Output] = builder.mapperPlatform[Input,Output]()
    platform.setComputation(this)
    platform.init(rconf, inputMap, outputMap, builder)
    // platform.setMapper(this)
    init(rconf, inputMap, outputMap, platform)
  }

  // def compute(input: Input): List[Output]
  /**
    * Given an input, this attempts to compute the list of outputs, stamp it with the version of the computation,
    * give it provenance information, and then put it in the output map to be sent further down the pipeline.
    * Calls [[bigglue.curators.VersionCurator.stampVersion]], [[bigglue.curators.ProvenanceCurator.reportProvenance]], and [[DataStore.put]].
    * @param input The input that needs to be computed.
    * @return If the computation succeeded, the list of outputs that were produced by the input.
    *         If the computation didn't succeed, it report the error and returns None.
    */
  def tryCompute(input: Input): Option[List[Output]] = {
    val platform = getUnaryPlatform()
    try {
      val outputs = op(input).map(
        output => {
          val voutput = platform.getVersionCurator().stampVersion(output)
          platform.getProvenanceCurator().reportProvenance(input, voutput)
          platform.getOutputMap().put(voutput)
          voutput
        }
      )
      platform.getUpstreamConnector().reportUp(Status.Done, input)
      //platform.getProvenanceCurator().reportProvenance(input, outputs)
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

  /**
    * This is part of the series of function calls that make the pipeline. In the example, this is called when stating (Clone()-->clonedMap) or (CommitExtraction()-->commitInfoMap).
    * Simply put, this creates a part of the pipeline that connects the mapper to the output data store and the rest of the
    * pipeline after that.
    * @param p The part of the pipe that follows the mapper computation. In the example, for Clone() it's clonedMap:--CommitExtraction()-->commitInfoMap:-+FindAuthor()+->authorMap,
    *          and for BB it's commitInfoMap:-+FindAuthor()+->authorMap.
    * @tparam End The type of data store that is at the end of the pipeline. In the example, this would be [[bigglue.examples.GitCommitGroups]]
    * @return A [[PartialMapperPipe]] which links the mapper together with the pipe that starts with the output data store.
    */
  def -->[End <: Identifiable[End]](p: Pipe[Output,End]): PartialMapperPipe[Input,Output,End] = PartialMapperPipe(this, p)

}
