package bigglue.platforms.instances.bigactors

import akka.actor.{Actor, ActorRef, Props}
import bigglue.computations.{Mapper, PairwiseComposer, Reducer}
import bigglue.configurations.{PipeConfig, PlatformBuilder}
import bigglue.data.{BasicIdentity, Identifiable, Identity}
import bigglue.exceptions.NotInitializedException
import bigglue.store.{DataMap, DataStore}

import scala.util.Random

class BigActorMapperPlatform [Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorUnaryPlatform[Input, Output] {
  override def compute_(job: Input): Unit = {
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

  override def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputMap, outputMap, builder)
  }

  override def compute_(job: Input): Unit = {
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
      case _ =>
        getErrorCurator().reportError(job, new Exception("Found unexpected computation. Expected: Reducer"))
    }
  }
}


class BigActorPairwiseComposerPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR],Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorBinaryPlatform[InputL,InputR,Output] {
  override def compute_(input: bigglue.data.Pair[InputL, InputR]): Unit = {
    computationOpt match{
      case Some(computation: PairwiseComposer[InputL, InputR, Output]) =>
        computation.tryFilterAndCompose(input)
        super.compute_(input)
      case _ =>
        getPairErrorCurator().reportError(input, new Exception("Found unexpected computation. Expected: Pairwise Composer"))
    }
  }
}

