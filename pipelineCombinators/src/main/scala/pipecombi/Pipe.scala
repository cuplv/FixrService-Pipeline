package pipecombi

import Implicits.DataNode
import pipecombi.Identifiable
import akka.actor.{Actor, ActorRef, ActorSystem, Props}

/**
  * Created by edmundlam on 6/23/17.
  */

/*
  DataMap  Dm[D]    Transformer  Tr[I,O]    Composer   Cr[L,R]

  Pipe        P[D]    ::=  Dm[D]                                        -- DataNode[D]
                       |   P[I]  :--Tr[I,O]-->  Dm[O]  D == O           -- TransformationPipe[I,O]
                       |   P[L]  <-*Cr[L,R]*->  P[R]   D == (L,R)       -- CompositionPipe[I,O]
                       |   P[I]  :<  l[I,O]            D == O           -- JunctionPipe[I,O]
                       |   P[L]  ||  P[R]              D == Either L R  -- ParallelPipe[L,R]

  Partial Pipe   l[I,D]   ::=   :--Tr[I,O]-->  Dm[O]    D == O           -- PartialTransformationPipe[I,O]
                           |    <-*Cr[L,R]*->  P[R]     D == (L,R)       -- PartialCompositionPipe[L,R]
                           |    l[I,O]  l[O,D]                           -- PartialHeadPipe[I,O,D]
                           |    l[I,L] ~ l[I,R]         D == Either L R  -- PartialParallelPipes[I,L,R]
*/


abstract class Pipe[Data <: Identifiable] {
  def :--[Next <: Identifiable](parTrans: PartialTransformationPipe[Data,Next]): Pipe[Next] = {
    TransformationPipe(this, parTrans.trans, parTrans.output)
  }
  def <-*[Other <: Identifiable](parComp: PartialCompositionPipe[Data,Other]): Pipe[pipecombi.Pair[Data,Other]] = {
    CompositionPipe(this, parComp.comp, parComp.inputR)
  }
  def :<[Next <: Identifiable](p: PartialPipe[Data,Next]): Pipe[Next] = {
    // p.completeWith(this)
    JunctionPipe(this, p)
  }
  def run(): DataMap[Data]
}

object Implicits {
  implicit class DataNode[Data <: Identifiable](map: DataMap[Data]) extends Pipe[Data] {
    override def toString: String = map.displayName
    override def run(): DataMap[Data] = {
      println(s"DataMap ${map.displayName} Computed")
      map
    }
  }
}

case class TransformationPipe[Input <: Identifiable, Output <: Identifiable]
      (input: Pipe[Input],  trans: Transformer[Input,Output], output: DataMap[Output]) extends Pipe[Output] {
  ActorSystem().actorOf(Props(new PipeActor(trans.stepActor, List(), output))) //Find a way to get the next step's actor somehow.
  override def toString: String = s"${input.toString} :--${trans.toString}--> ${output.displayName}"
  override def run(): DataMap[Output] = {
    val map = trans.process(input.run(),output)
    println(s"Computed transformation ${trans.toString} and deposited data in ${output.displayName}")
    map
  }
}

case class CompositionPipe[InputL <: Identifiable, InputR <: Identifiable]
      (inputL: Pipe[InputL], comp: Composer[InputL,InputR], inputR: Pipe[InputR]) extends Pipe[pipecombi.Pair[InputL,InputR]] {
  override def toString: String = s"${inputL.toString} <-*${comp.toString}*-> ${inputR.toString}"
  override def run(): DataMap[pipecombi.Pair[InputL,InputR]] = {
    val map = comp.compose(inputL.run(), inputR.run())
    println(s"Computed composition ${comp.toString}")
    map
  }
}

case class ParallelPipes[OutputL <: Identifiable, OutputR <: Identifiable]
      (outputL: Pipe[OutputL], outputR: Pipe[OutputR]) extends Pipe[pipecombi.Either[OutputL,OutputR]] {

  override def run(): DataMap[pipecombi.Either[OutputL,OutputR]] = {
    val mapL: DataMap[OutputL] = outputL.run()
    val mapR: DataMap[OutputR] = outputR.run()
    EitherDataMap(mapL,mapR)
  }
}

case class JunctionPipe[Input <: Identifiable, Output <: Identifiable](input: Pipe[Input], output: PartialPipe[Input, Output]) extends Pipe[Output] {
  override def toString: String = s"${input.toString} :< { ${output.toString} }"
  override def run(): DataMap[Output] = {
    output.completeWith(new DataNode(input.run())).run()
  }
}


// Partial Pipes

abstract class PartialPipe[Input <: Identifiable, Data <: Identifiable] {
   def completeWith(input: Pipe[Input]): Pipe[Data]
   def :--[Next <: Identifiable](p: PartialTransformationPipe[Data,Next]): PartialPipe[Input,Next] = PartialHeadPipe(this, p)
   def <-*[Other <: Identifiable](p: PartialCompositionPipe[Data,Other]): PartialPipe[Input,pipecombi.Pair[Data,Other]] = PartialHeadPipe(this, p)
   def ~[Other <: Identifiable](other: PartialPipe[Input,Other]): PartialPipe[Input,pipecombi.Either[Data,Other]] = ParallelPartialPipes(this, other)
}

case class PartialTransformationPipe[Input <: Identifiable, Output <: Identifiable]
      (trans: Transformer[Input,Output], output: DataMap[Output]) extends PartialPipe[Input,Output] {
   override def completeWith(input: Pipe[Input]): Pipe[Output] = TransformationPipe(input, trans, output)
   override def toString: String = s":--${trans.toString}--> ${output.displayName}"
}

case class PartialCompositionPipe[InputL <: Identifiable, InputR <: Identifiable]
      (comp: Composer[InputL,InputR], inputR: Pipe[InputR]) extends PartialPipe[InputL,pipecombi.Pair[InputL,InputR]] {
   override def completeWith(inputL: Pipe[InputL]): Pipe[pipecombi.Pair[InputL,InputR]] = CompositionPipe(inputL, comp, inputR)
   override def toString: String = s"<-*${comp.toString}*-> ${inputR.toString}"
}

case class PartialHeadPipe[Input <: Identifiable, Inter <: Identifiable, Output <: Identifiable]
     (head: PartialPipe[Input,Inter], rest: PartialPipe[Inter,Output]) extends PartialPipe[Input, Output] {
   override def completeWith(input: Pipe[Input]): Pipe[Output] = rest.completeWith(head.completeWith(input))
   override def toString: String = s"${head.toString} ${rest.toString}"
}

case class ParallelPartialPipes[Input <: Identifiable, Output1 <: Identifiable, Output2 <: Identifiable]
      (p1: PartialPipe[Input, Output1], p2: PartialPipe[Input, Output2]) extends PartialPipe[Input, pipecombi.Either[Output1,Output2]] {
  override def completeWith(input: Pipe[Input]): Pipe[pipecombi.Either[Output1, Output2]] = ParallelPipes( p1.completeWith(input), p2.completeWith(input) )
  override def toString: String = s"\n   ${p1.toString}\n   ${p2.toString}"
}

class PipeActor[Input <: Identifiable, Output <: Identifiable](currStep: ActorRef, nextSteps: List[ActorRef], outputMap: DataMap[Output]) extends Actor {
  def receive: Receive = {
    case (d: DataMap[Input], "input") => currStep ! (d, outputMap, self)
    case (d: DataMap[Input], n: Int, "input") => currStep ! (d, outputMap, n, self)
    case (d: DataMap[Output], "output") =>
      nextSteps.foldLeft(){
        case (_, nextStep) => nextStep ! (d, "input")
      }
    case _ => ()
  }
}