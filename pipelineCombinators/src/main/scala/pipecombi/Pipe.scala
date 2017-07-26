package pipecombi

import Implicits.DataNode
import mthread_abstrac.{MPipelineAbstraction, MPipelineBuilder, MThreadAbstraction}
//import pipecombi.Identifiable //UNNEEDED IMPORT STATEMENT

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
  def stRun(): DataMap[Data]
  def run(): Unit = {
    val pipeline = MPipelineBuilder.build(None)
    pipeline.run(pipeline.build(build))
  }
  def build: Map[String, Any]
}

object Implicits {
  implicit class DataNode[Data <: Identifiable](map: DataMap[Data])(implicit parallel: Boolean = false) extends Pipe[Data] {
    val dMap: DataMap[Data] = map

    override def toString: String = map.displayName
    override def stRun(): DataMap[Data] = {
      println(s"DataMap ${map.displayName} Computed")
      map
    }

    override def build: Map[String, Any] = {
      val startMap = Map("PipeType" -> "DataNode")
      if (parallel) startMap else startMap + ("OutputMap" -> dMap)
    }

  }
}

case class TransformationPipe[Input <: Identifiable, Output <: Identifiable]
      (input: Pipe[Input],  trans: Transformer[Input,Output], output: DataMap[Output]) extends Pipe[Output] {

  override def toString: String = s"${input.toString} :--${trans.toString}--> ${output.displayName}"
  override def stRun(): DataMap[Output] = {
    val map = trans.process(input.stRun(),output)
    println(s"Computed transformation ${trans.toString} and deposited data in ${output.displayName}")
    map
  }

  override def build: Map[String, Any] = Map("PipeType" -> "TransformationPipe",
    "StepAbstraction" -> trans.stepAbstract,
    "OutputMap" -> output,
    "input" -> input.build)

}

case class CompositionPipe[InputL <: Identifiable, InputR <: Identifiable]
      (inputL: Pipe[InputL], comp: Composer[InputL,InputR], inputR: Pipe[InputR]) extends Pipe[pipecombi.Pair[InputL,InputR]] {

  override def toString: String = s"${inputL.toString} <-*${comp.toString}*-> ${inputR.toString}"
  override def stRun(): DataMap[pipecombi.Pair[InputL,InputR]] = {
    val map = comp.compose(inputL.stRun(), inputR.stRun())
    println(s"Computed composition ${comp.toString}")
    map
  }

  override def build: Map[String, Any] = {
    val compose: (DataMap[InputL], DataMap[InputR], Option[DataMap[pipecombi.Pair[InputL, InputR]]]) => DataMap[pipecombi.Pair[InputL, InputR]] = comp.compose
    Map("PipeType" -> "CompositionPipe", "composer" -> compose, "inputL" -> inputL.build, "inputR" -> inputR.build)
  }
}

case class ParallelPipes[OutputL <: Identifiable, OutputR <: Identifiable]
      (outputL: Pipe[OutputL], outputR: Pipe[OutputR]) extends Pipe[pipecombi.Either[OutputL,OutputR]] {
  
  override def stRun(): DataMap[pipecombi.Either[OutputL,OutputR]] = {
    val mapL: DataMap[OutputL] = outputL.stRun()
    val mapR: DataMap[OutputR] = outputR.stRun()
    EitherDataMap(mapL,mapR)
  }

  override def build: Map[String, Any] = {
    Map("PipeType" -> "ParallelPipes",
      "LeftPipe" -> outputL.build,
      "RightPipe" -> outputR.build)
  }
}

case class JunctionPipe[Input <: Identifiable, Output <: Identifiable](input: Pipe[Input], output: PartialPipe[Input, Output]) extends Pipe[Output] {
  override def toString: String = s"${input.toString} :< { ${output.toString} }"
  override def stRun(): DataMap[Output] = {
    output.completeWith(new DataNode(input.stRun())).stRun()
  }

  override def build: Map[String, Any] = {
    Map("PipeType" -> "JunctionPipe",
      "OutputPipes" -> output.completeWith(new DataNode(new InMemDataMap[Input]())(true)).build,
      "input" -> input.build)
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
