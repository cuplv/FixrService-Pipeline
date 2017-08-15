package protopipes.platforms.instances.bigactors

import protopipes.computations.{Mapper, PairwiseComposer, Reducer}
import protopipes.connectors.Status
import protopipes.data.{Identifiable, Identity}
import protopipes.platforms.{ComputesMap, ComputesPairwiseCompose, ComputesReduce}
import protopipes.store.DataMap

import scala.util.Random

class BigActorMapperPlatform [Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorUnaryPlatform[Input, Output] {
  override def compute(job: Input): Unit = {
    computationOpt match{
      case Some(computation: Mapper[Input, Output]) =>
        computation.tryCompute(job)
      case _ =>
        getErrorCurator().reportError(job, new Exception("Found unexpected computation. Expected: Mapper"))
    }
  }
}

class BigActorReducerPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorUnaryPlatform[Input, Output] {
  var mergals = Map.empty[Identity[Output], Seq[Input]]
  override def compute(job: Input): Unit = {
    computationOpt match{
      case Some(computation: Reducer[Input, Output]) =>
        computation.tryGroupBy(job) match{
          case Some(outputId) =>
            val outputVal = mergals.getOrElse (outputId, Seq.empty[Input]) :+ job
            mergals = mergals + (outputId -> outputVal )
            mergals.get(outputId) match{
              case Some(x) =>
                computation.tryZero() match {
                  case Some(zero) =>
                    val outputMap = getOutputMap().asInstanceOf[DataMap[Identity[Output],Output]]
                    val output = outputMap.getOrElse(outputId, zero)
                    outputMap.put(outputVal.foldRight(output){ (i,o) => computation.tryFold(i,o) match{
                      case Some(no) => no
                      case None => o
                    }})
                  case None => ()
                }
              case None =>
                getErrorCurator().reportError(job, new Exception("Literally impossible to get to this point."))
            }
          case None => ()
        }
      case None =>
        getErrorCurator().reportError(job, new Exception("Found unexpected computation. Expected: Reducer"))
    }
  }
}

class BigActorPairwiseComposerPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR],Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorBinaryPlatform[InputL,InputR,Output] {
  override def compute(input: protopipes.data.Pair[InputL, InputR]): Unit = {
    computationOpt match{
      case Some(computation: PairwiseComposer[InputL, InputR, Output]) =>
        computation.tryFilterAndCompose(input)
        super.compute(input)
      case None =>
        getPairErrorCurator().reportError(input, new Exception("Found unexpected computation. Expected: Pairwise Composer"))
    }
  }
}

