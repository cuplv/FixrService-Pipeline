package protopipes.platforms.instances.bigactors

import akka.actor.{Actor, ActorRef, Props}
import protopipes.computations.{Mapper, PairwiseComposer, Reducer}
import protopipes.configurations.{PipeConfig, PlatformBuilder}
import protopipes.data.{BasicIdentity, Identifiable, Identity}
import protopipes.exceptions.NotInitializedException
import protopipes.store.{DataMap, DataStore}

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
  var reducerComputerOpt: Option[ActorRef] = None

  def reducerComputer: ActorRef = reducerComputerOpt match{
    case Some(actorRef) => actorRef
    case None => throw new NotInitializedException("reducerComputer", "Reducing", None)
  }

  override def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputMap, outputMap, builder)
    reducerComputerOpt = Some(actorSystem.actorOf(Props(classOf[BigActorReducerWorker[Input, Output]], this), "reducerComputer"))
  }

  override def compute(job: Input): Unit = reducerComputer ! job

  def compute_(job: Input): Unit = {
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

class BigActorReducerWorker[Input <: Identifiable[Input], Output <: Identifiable[Output]]
  (platform: BigActorReducerPlatform[Input, Output]) extends Actor{

  def receive: Receive = {
    case job: Input @ unchecked => platform.compute_(job)
  }
}


class BigActorPairwiseComposerPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR],Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + Random.nextInt(99999)) extends BigActorBinaryPlatform[InputL,InputR,Output] {
  override def compute(input: protopipes.data.Pair[InputL, InputR]): Unit = {
    computationOpt match{
      case Some(computation: PairwiseComposer[InputL, InputR, Output]) =>
        computation.tryFilterAndCompose(input)
        super.compute(input)
      case _ =>
        getPairErrorCurator().reportError(input, new Exception("Found unexpected computation. Expected: Pairwise Composer"))
    }
  }
}

