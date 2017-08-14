import protopipes.connectors.Status
import protopipes.data.{Identifiable, Identity}
import protopipes.platforms.{ComputesMap, ComputesPairwiseCompose, ComputesReduce}
import protopipes.platforms.instances.bigactors.{BigActorBinaryPlatform, BigActorPlatform, BigActorUnaryPlatform}
import protopipes.platforms.instances.thinactors.ThinActorBinaryPlatform
import protopipes.store.DataMap

import scala.util.Random

class BigActorMapperPlatform [Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorUnaryPlatform[Input, Output] with ComputesMap[Input, Output] {

  override def compute(job: Input): Unit = {
    val mapper = getMapper()
    val upstreamConnector = getUpstreamConnector()
    val outputMap = getOutputMap()
    tryCompute(upstreamConnector, mapper, job, outputMap)
  }
}

class BigActorReducerPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorUnaryPlatform[Input, Output] with ComputesReduce[Input,Output] {
  var mergals = Map.empty[Identity[Output],Seq[Input]]
  override def compute(job: Input): Unit = {
    val upstreamConnector = getUpstreamConnector()
    val reducer = getReducer()
    tryGroup(upstreamConnector, reducer, job) match {
      case Some(outputId) =>
        val outputVal = mergals.getOrElse(outputId, Seq.empty[Input]) :+ job
        mergals += (outputId -> outputVal)
        val outputMap = getOutputMap().asInstanceOf[DataMap[Identity[Output],Output]]
        val output = outputMap.getOrElse(outputId, reducer.zero())
        outputMap.put(outputVal.foldRight(output){ (i,o) => tryFold(upstreamConnector, reducer, i, o) })
      case None => () // Error occurred. Ignore
    }
  }
}

class BigActorPairwiseComposerPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR],Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorBinaryPlatform[InputL,InputR,Output]  with ComputesPairwiseCompose[InputL,InputR,Output] {
  override def compute(input: protopipes.data.Pair[InputL, InputR]): Unit = {
    val outputMap = getOutputMap()
    tryFilterAndCompose(getPairConnector(), getComposer(), input, outputMap)
    getUpstreamLConnector().reportUp(Status.Done, input.left)
    getUpstreamRConnector().reportUp(Status.Done, input.right)
  }
}

