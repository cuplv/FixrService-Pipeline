package pipecombi

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

/**
  * Created by edmundlam on 6/25/17.
  */
abstract class Composer[InputL <: Identifiable, InputR <: Identifiable](implicit system: ActorSystem) extends Operator[InputL, InputR, pipecombi.Pair[InputL,InputR]] {
  def compose(lFeat: DataMap[InputL], rFeat: DataMap[InputR]): DataMap[pipecombi.Pair[InputL,InputR]]

  def compose(lFeat: DataMap[InputL], rFeat: DataMap[InputR], currMap: DataMap[pipecombi.Pair[InputL,InputR]]): DataMap[pipecombi.Pair[InputL,InputR]]

  override def operate(arg1: DataMap[InputL], arg2: DataMap[InputR]): DataMap[pipecombi.Pair[InputL,InputR]] = compose(arg1,arg2)

  // def *-> (inputR: DataMap[InputR]): Composition[InputL, InputR] = Composition(this, inputR)

  def *->(inputR: Pipe[InputR]): PartialCompositionPipe[InputL, InputR] = PartialCompositionPipe(this, inputR)
}

case class BatchProduct[L <: Identifiable,R <: Identifiable]()(implicit system: ActorSystem) extends Composer[L, R] {
  override val version = "0.1"

  override val statMap = new InMemDataMap[Stat]()
  override val provMap = new InMemDataMap[Identity]()
  override val errMap = new InMemDataMap[ErrorSummary]()

  override def compose(mapL: DataMap[L], mapR: DataMap[R]): DataMap[pipecombi.Pair[L, R]] = {
    val outMap = new InMemDataMap[pipecombi.Pair[L,R]]()
    mapL.items.map {
      inputL => mapR.items.map {
        inputR => { outMap.put(pipecombi.Pair(inputL, inputR)) }
      }
    }
    outMap
  }

  override def compose(mapL: DataMap[L], mapR: DataMap[R], currMap: DataMap[pipecombi.Pair[L,R]]): DataMap[pipecombi.Pair[L,R]] = {
    mapL.items.map{
      inputL =>
        val comp = mapR.identities.head
        currMap.get((inputL.identity() |+| comp).identity()) match {
          case Some(x) => true
          case None => mapR.items.map {
            inputR => {currMap.put(pipecombi.Pair(inputL, inputR))}
          }
        }
    }
    mapR.items.map{
      inputR =>
        val comp = mapL.identities.head
        currMap.get((inputR.identity() |+| comp).identity()) match{
          case Some(x) => true
          case None => mapL.items.map {
            inputL => {currMap.put(pipecombi.Pair(inputL, inputR))}
          }
        }
    }
    currMap
  }

  override def toString: String = "BatchProduct"
}

object BatchProduct {
  def composer[L <: Identifiable,R <: Identifiable]()(implicit system: ActorSystem): Composer[L,R] = BatchProduct[L,R]()(system)
}

/*
case class Composition[InputL <: Identifiable, InputR <: Identifiable](comp: Composer[InputL, InputR], inputR: DataMap[InputR]) {
} */