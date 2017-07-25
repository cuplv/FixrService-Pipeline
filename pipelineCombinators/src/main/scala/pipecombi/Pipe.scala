package pipecombi

import Implicits.DataNode
import mthread_abstrac.MThreadAbstraction
//import pipecombi.Identifiable //UNNEEDED IMPORT STATEMENT
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
  val aRef: Option[ActorRef] = None
  def :--[Next <: Identifiable](parTrans: PartialTransformationPipe[Data,Next])(implicit system: ActorSystem): Pipe[Next] = {
    TransformationPipe(this, parTrans.trans, parTrans.output)
  }
  def <-*[Other <: Identifiable](parComp: PartialCompositionPipe[Data,Other])(implicit system: ActorSystem): Pipe[pipecombi.Pair[Data,Other]] = {
    CompositionPipe(this, parComp.comp, parComp.inputR)
  }
  def :<[Next <: Identifiable](p: PartialPipe[Data,Next])(implicit system: ActorSystem): Pipe[Next] = {
    // p.completeWith(this)
    JunctionPipe(this, p)
  }
  def run(): DataMap[Data]
  def run(refs: List[(ActorRef, String)]) = {
    refs.foreach{
      case (ref, "") => ref ! "output"
      case _ => ()
    }
  }
  def build(nextSteps: List[(ActorRef, String)] = List(), firstSteps: List[(ActorRef, String)] = List()): List[(ActorRef, String)] = List.empty[(ActorRef, String)]
}

object Implicits {
  implicit class DataNode[Data <: Identifiable](map: DataMap[Data])(implicit system: ActorSystem, parallel: Boolean = false) extends Pipe[Data] {
    val dMap: DataMap[Data] = map
    override val aRef =
      if (!parallel) Some(system.actorOf(Props(new SimplePipeActor[Data](this)))) else None
    override def toString: String = map.displayName
    override def run(): DataMap[Data] = {
      println(s"DataMap ${map.displayName} Computed")
      map
    }

    override def build(nextSteps: List[(ActorRef, String)] = List(), firstSteps: List[(ActorRef, String)] = List()): List[(ActorRef, String)] = {
      aRef match {
        case Some(acRef) =>
          nextSteps foreach { nextStep => acRef ! nextStep }
          (acRef, "") :: firstSteps
        case None =>
          nextSteps.foldRight(firstSteps){
            case ((nextStep, code), list) => (nextStep, code+"P") :: list
          }
      }
    }
  }
}

case class TransformationPipe[Input <: Identifiable, Output <: Identifiable]
      (input: Pipe[Input],  trans: Transformer[Input,Output], output: DataMap[Output])(implicit system: ActorSystem) extends Pipe[Output] {
  override val aRef = Some(system.actorOf(Props(new PipeActor(trans.toString, trans.stepAbstract, output))))
  override def toString: String = s"${input.toString} :--${trans.toString}--> ${output.displayName}"
  override def run(): DataMap[Output] = {
    val map = trans.process(input.run(),output, List())
    println(s"Computed transformation ${trans.toString} and deposited data in ${output.displayName}")
    map
  }

  override def build(nextSteps: List[(ActorRef, String)] = List(), firstSteps: List[(ActorRef, String)] = List()): List[(ActorRef, String)] = {
    val acRef = aRef.value
    nextSteps.foreach{ nextStep => acRef ! nextStep; acRef ! (output, "dataMap"+nextStep._2) }
    input.build(List((acRef, "nextStep")), firstSteps)
  }
}

case class CompositionPipe[InputL <: Identifiable, InputR <: Identifiable]
      (inputL: Pipe[InputL], comp: Composer[InputL,InputR], inputR: Pipe[InputR])(implicit system: ActorSystem) extends Pipe[pipecombi.Pair[InputL,InputR]] {
  override val aRef = Some(system.actorOf(Props(new ComposedPipeActor(comp))))
  override def toString: String = s"${inputL.toString} <-*${comp.toString}*-> ${inputR.toString}"
  override def run(): DataMap[pipecombi.Pair[InputL,InputR]] = {
    val map = comp.compose(inputL.run(), inputR.run())
    println(s"Computed composition ${comp.toString}")
    map
  }

  override def build(nextSteps: List[(ActorRef, String)] = List(), firstSteps: List[(ActorRef, String)] = List()): List[(ActorRef, String)] = {
    val acRef = aRef.value
    nextSteps.foreach{ nextStep => acRef ! nextStep; } //acRef ! (output, "dataMap"+nextStep._2) }
    inputR.build(List((acRef, "nextStepR")), inputL.build(List((acRef, "nextStepL")), firstSteps))
  }

}

case class ParallelPipes[OutputL <: Identifiable, OutputR <: Identifiable]
      (outputL: Pipe[OutputL], outputR: Pipe[OutputR])(implicit system: ActorSystem) extends Pipe[pipecombi.Either[OutputL,OutputR]] {
  
  override def run(): DataMap[pipecombi.Either[OutputL,OutputR]] = {
    val mapL: DataMap[OutputL] = outputL.run()
    val mapR: DataMap[OutputR] = outputR.run()
    EitherDataMap(mapL,mapR)
  }

  override def build(nextSteps: List[(ActorRef, String)], firstSteps: List[(ActorRef, String)]): List[(ActorRef, String)] = {
    outputR.build(nextSteps, outputL.build(nextSteps, firstSteps))
  }
}

case class JunctionPipe[Input <: Identifiable, Output <: Identifiable](input: Pipe[Input], output: PartialPipe[Input, Output])(implicit system: ActorSystem) extends Pipe[Output] {
  override def toString: String = s"${input.toString} :< { ${output.toString} }"
  override def run(): DataMap[Output] = {
    output.completeWith(new DataNode(input.run())).run()
  }

  override def build(nextSteps: List[(ActorRef, String)], firstSteps: List[(ActorRef, String)]): List[(ActorRef, String)] = {
    val newPipe = output.completeWith(new DataNode(new InMemDataMap[Input]())(system, true))
    val inTheJunction = newPipe.build(List(), List())
    val (newNextSteps, newFirstSteps) = inTheJunction.foldRight((nextSteps, firstSteps)){
      case ((a, "nextStepP"), (newSteps, currSteps)) => ((a, "nextStep") :: newSteps, currSteps)
      case ((a, "nextStepLP"), (newSteps, currSteps)) => ((a, "nextStepL") :: newSteps, currSteps)
      case ((a, "nextStepRP"), (newSteps, currSteps)) => ((a, "nextStepR") :: newSteps, currSteps)
      case ((a, s), (newSteps, currSteps)) => (newSteps, (a, s) :: currSteps)
      case (_, x) => x
    }
    input.build(newNextSteps, newFirstSteps)
  }
}


// Partial Pipes

abstract class PartialPipe[Input <: Identifiable, Data <: Identifiable](implicit system: ActorSystem) {
   def completeWith(input: Pipe[Input]): Pipe[Data]
   def :--[Next <: Identifiable](p: PartialTransformationPipe[Data,Next]): PartialPipe[Input,Next] = PartialHeadPipe(this, p)
   def <-*[Other <: Identifiable](p: PartialCompositionPipe[Data,Other]): PartialPipe[Input,pipecombi.Pair[Data,Other]] = PartialHeadPipe(this, p)
   def ~[Other <: Identifiable](other: PartialPipe[Input,Other]): PartialPipe[Input,pipecombi.Either[Data,Other]] = ParallelPartialPipes(this, other)
}

case class PartialTransformationPipe[Input <: Identifiable, Output <: Identifiable]
      (trans: Transformer[Input,Output], output: DataMap[Output])(implicit system: ActorSystem) extends PartialPipe[Input,Output] {
   override def completeWith(input: Pipe[Input]): Pipe[Output] = TransformationPipe(input, trans, output)
   override def toString: String = s":--${trans.toString}--> ${output.displayName}"
}

case class PartialCompositionPipe[InputL <: Identifiable, InputR <: Identifiable]
      (comp: Composer[InputL,InputR], inputR: Pipe[InputR])(implicit system: ActorSystem) extends PartialPipe[InputL,pipecombi.Pair[InputL,InputR]] {
   override def completeWith(inputL: Pipe[InputL]): Pipe[pipecombi.Pair[InputL,InputR]] = CompositionPipe(inputL, comp, inputR)
   override def toString: String = s"<-*${comp.toString}*-> ${inputR.toString}"
}

case class PartialHeadPipe[Input <: Identifiable, Inter <: Identifiable, Output <: Identifiable]
     (head: PartialPipe[Input,Inter], rest: PartialPipe[Inter,Output])(implicit system: ActorSystem) extends PartialPipe[Input, Output] {
   override def completeWith(input: Pipe[Input]): Pipe[Output] = rest.completeWith(head.completeWith(input))
   override def toString: String = s"${head.toString} ${rest.toString}"
}

case class ParallelPartialPipes[Input <: Identifiable, Output1 <: Identifiable, Output2 <: Identifiable]
      (p1: PartialPipe[Input, Output1], p2: PartialPipe[Input, Output2])(implicit system: ActorSystem) extends PartialPipe[Input, pipecombi.Either[Output1,Output2]] {
  override def completeWith(input: Pipe[Input]): Pipe[pipecombi.Either[Output1, Output2]] = ParallelPipes( p1.completeWith(input), p2.completeWith(input) )
  override def toString: String = s"\n   ${p1.toString}\n   ${p2.toString}"
}

class PipeActor[Input <: Identifiable, Output <: Identifiable](name: String, currStep: MThreadAbstraction, outputMap: DataMap[Output]) extends Actor {
  import context._
  become(haveNextSteps(List()))
  def receive: Receive = {
    case _ => ()
  }

  def haveNextSteps(nextSteps: List[(ActorRef, String)], givenMap: Boolean = false): Receive = {
    case (acRef: ActorRef, "nextStep") => become(haveNextSteps((acRef, "") :: nextSteps, givenMap))
    case (acRef: ActorRef, "nextStepL") => become(haveNextSteps((acRef, "L") :: nextSteps, givenMap))
    case (acRef: ActorRef, "nextStepR") => become(haveNextSteps((acRef, "R") :: nextSteps, givenMap))
    case (d: DataMap[Input], "input") =>
      currStep ! ("init", d, outputMap, self)
      currStep ! "input"
    case ("input") =>
      currStep ! "input"
    //case (d: DataMap[Input], n: Int, "input") => currStep ! (d, outputMap, n, self)
    case "output" =>
      println(s"Computed transformation ${name} and deposited data in ${outputMap.displayName}")
      if (!givenMap) {
        nextSteps.foldLeft() {
          case (_, (nextStep, "")) => nextStep ! (outputMap, "input")
          case (_, (nextStep, "L")) => nextStep ! (outputMap, "inputL")
          case (_, (nextStep, "R")) => nextStep ! (outputMap, "inputR")
        }
        become(haveNextSteps(nextSteps, givenMap = true))
      } else {
        nextSteps.foldLeft() {
          case (_, (nextStep, "")) => nextStep ! "input"
          case (_, (nextStep, "L")) => nextStep ! "inputL"
          case (_, (nextStep, "R")) => nextStep ! "inputR"
        }
      }
    case _ => ()
  }
}

class ComposedPipeActor[InputL <: Identifiable, InputR <: Identifiable](currStep: Composer[InputL, InputR]) extends Actor {
  import context._
  become(dataMapNoGotten(List()))
  def composeAndSend(dML: DataMap[InputL], dMR: DataMap[InputR],
                     nextSteps: List[(ActorRef, String)]): DataMap[pipecombi.Pair[InputL, InputR]] = {
    val dMap = currStep.compose(dML, dMR)
    nextSteps.foldLeft(){
      case (_, (nextStep, "")) => nextStep ! (dMap, "input")
      case (_, (nextStep, "L")) => nextStep ! (dMap, "inputL")
      case (_, (nextStep, "R")) => nextStep ! (dMap, "inputR")
    }
    println(s"Computed composition ${currStep.toString}")
    dMap
  }

  def composeAndSend(dML: DataMap[InputL], dMR: DataMap[InputR], dMOut: DataMap[pipecombi.Pair[InputL, InputR]],
                    nextSteps: List[(ActorRef, String)]): Unit = {
    val dMap = currStep.compose(dML, dMR, dMOut)
    nextSteps.foldLeft(){
      case (_, (nextStep, "")) => nextStep ! "input"
      case (_, (nextStep, "L")) => nextStep ! "inputL"
      case (_, (nextStep, "R")) => nextStep ! "inputR"
    }
  }

  def dataMapLGotten(dML: DataMap[InputL], nextSteps: List[(ActorRef, String)]): Receive = {
    case (d: DataMap[InputL], "inputL") => become(dataMapLGotten(d,  nextSteps))
    case (d: DataMap[InputR], "inputR") =>
      become(dataMapLRGotten(dML, d, composeAndSend(dML, d, nextSteps), nextSteps))
    case (acRef: ActorRef, "nextStep") => become(dataMapLGotten(dML, (acRef, "") :: nextSteps))
    case (acRef: ActorRef, "nextStepL") => become(dataMapLGotten(dML, (acRef, "L") :: nextSteps))
    case (acRef: ActorRef, "nextStepR") => become(dataMapLGotten(dML, (acRef, "R") :: nextSteps))
    case "inputL" => ()
    case _ => ()
  }

  def dataMapRGotten(dMR: DataMap[InputR], nextSteps: List[(ActorRef, String)]): Receive = {
    case (d: DataMap[InputL], "inputL") =>
      become(dataMapLRGotten(d, dMR, composeAndSend(d, dMR, nextSteps), nextSteps))
    case (d: DataMap[InputR], "inputR") => become(dataMapRGotten(d, nextSteps))
    case (acRef: ActorRef, "nextStep") => become(dataMapRGotten(dMR, (acRef, "") :: nextSteps))
    case (acRef: ActorRef, "nextStepL") => become(dataMapRGotten(dMR, (acRef, "L") :: nextSteps))
    case (acRef: ActorRef, "nextStepR") => become(dataMapRGotten(dMR, (acRef, "R") :: nextSteps))
    case "inputR" => ()
    case _ => ()
  }

  def dataMapLRGotten(dML: DataMap[InputL], dMR: DataMap[InputR], dMOut: DataMap[pipecombi.Pair[InputL, InputR]], nextSteps: List[(ActorRef, String)]): Receive = {
    case (d: DataMap[InputL], "inputL") => become(dataMapLRGotten(d, dMR, dMOut, nextSteps))
    case (d: DataMap[InputR], "inputR") => become(dataMapLRGotten(dML, d, dMOut, nextSteps))
    case (acRef: ActorRef, "nextStep") => become(dataMapLRGotten(dML, dMR, dMOut, (acRef, "") :: nextSteps))
    case (acRef: ActorRef, "nextStepL") => become(dataMapLRGotten(dML, dMR, dMOut, (acRef, "L") :: nextSteps))
    case (acRef: ActorRef, "nextStepR") => become(dataMapLRGotten(dML, dMR, dMOut, (acRef, "R") :: nextSteps))
    case "inputL" | "inputR" => composeAndSend(dML, dMR, dMOut, nextSteps)
    case (d: DataMap[pipecombi.Pair[InputL, InputR]] @ unchecked, "composed") =>
      nextSteps.foldLeft(){
        case (_, (nextStep, "")) => nextStep ! (d, "input")
        case (_, (nextStep, "L")) => nextStep ! (d, "inputL")
        case (_, (nextStep, "R")) => nextStep ! (d, "inputR")
      }
    case _ => ()
  }

  def dataMapNoGotten(nextSteps: List[(ActorRef, String)]): Receive = {
    case (d: DataMap[InputL], "inputL") => become(dataMapLGotten(d, nextSteps))
    case (d: DataMap[InputR], "inputR") => become(dataMapRGotten(d, nextSteps))
    case (acRef: ActorRef, "nextStep") => become(dataMapNoGotten((acRef, "") :: nextSteps))
    case (acRef: ActorRef, "nextStepL") => become(dataMapNoGotten((acRef, "L") :: nextSteps))
    case (acRef: ActorRef, "nextStepR") => become(dataMapNoGotten((acRef, "R") :: nextSteps))
    case _ => ()
  }

  def receive: Receive = {
    case _ => ()
  }
}

class SimplePipeActor[Data <: Identifiable](d: DataNode[Data]) extends Actor {
  import context._
  become(haveNextSteps(List()))
  def receive: Receive = {
    case _ => ()
  }

  def haveNextSteps(nextSteps: List[(ActorRef, String)]): Receive = {
    case (acRef: ActorRef, "nextStep") => become(haveNextSteps((acRef, "") :: nextSteps))
    case (acRef: ActorRef, "nextStepL") => become(haveNextSteps((acRef, "L") :: nextSteps))
    case (acRef: ActorRef, "nextStepR") => become(haveNextSteps((acRef, "R") :: nextSteps))
    case "output" => nextSteps.foreach{
      case (nextStep, "") => nextStep ! (d.dMap, "input")
      case (nextStep, "L") =>
        nextStep ! (d.dMap, "inputL")
      case (nextStep, "R") =>
        nextStep ! (d.dMap, "inputR")
      case _ => ()
    }
      println(s"DataMap ${d.dMap.displayName} Computed")
  }
}