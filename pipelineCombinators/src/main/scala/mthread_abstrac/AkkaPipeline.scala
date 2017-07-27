package mthread_abstrac

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.Config

/**
  * Created by chanceroberts on 7/25/17.
  */
object AkkaPipelineBuilder{
  def getSystem: ActorSystem = {
    ActorSystem()
  }
}

class AkkaPipeline(system: ActorSystem) extends MPipelineAbstraction[ActorRef] {
  override def build(listOfSteps: Map[String, Any], nextSteps: List[(String, ActorRef)] = List(), firstSteps: List[(String, ActorRef)] = List()): List[(String, ActorRef)] = {
    listOfSteps.get("PipeType") match {
      case Some("JunctionPipe") =>
        val l: List[(String, ActorRef)] = listOfSteps.get("OutputPipes") match{
          case Some(m: Map[String @ unchecked, _]) => build(m, List(), List())
          case _ => List()
        }
        val nextSteps = l.foldRight(List.empty[(String, ActorRef)]){
          case (("" | "?", aRef), next) => ("nextStep?", aRef) :: next
          case (("P", aRef), next) => ("nextStep", aRef) :: next
          case (("LP", aRef), next) => ("nextStepL", aRef) :: next
          case (("RP", aRef), next) => ("nextStepR", aRef) :: next
        }
        listOfSteps.get("input") match{
          case Some(m: Map[String @ unchecked, _]) => build(m, nextSteps, firstSteps)
          case _ => firstSteps
        }
      case Some("TransformationPipe") =>
        val mTA = listOfSteps.get("StepAbstraction") match{
          case Some(m: MThreadAbstraction) => m
          case _ => return firstSteps //Please fix?
        }
        val dataMap = listOfSteps.get("OutputMap") match{
          case Some(x) => x
          case None => return firstSteps //Please fix?
        }
        val act = system.actorOf(Props(new AkkaPipePiece(mTA, dataMap)))
        /*
        val newFirstSteps = firstSteps.foldRight(List.empty[((String, Any), ActorRef)]){
          case ((("?", dMap), a), list) =>
            if (dMap.equals(dataMap)){
              act ! ("nextStep", a)
              list
            } else {
              (("?", dMap), a) :: list
            }
          case (other, list) => other :: list
        }
        */
        nextSteps.foreach{
          case ("nextStep", a) => act ! ("nextStep", a); a ! ("init", dataMap)
          case ("nextStepL", a) => act ! ("nextStepL", a); a ! ("initL", dataMap)
          case ("nextStepR", a) => act ! ("nextStepR", a); a ! ("initR", dataMap)
          case ("nextStep?", a) => act ! ("nextStep", a)
          case _ => ()
        }
        listOfSteps.get("input") match{
          case Some(m: Map[String @ unchecked, _]) => build(m, List(("nextStep", act)), firstSteps)
          case _ => firstSteps
        }
      case Some("CompositionPipe") =>
        val compute = listOfSteps.get("composer") match{
          case Some(a: ((Any, Any, Option[Any]) => Any)) => a
          case _ => return firstSteps //Please fix?
        }
        val act = system.actorOf(Props(new AkkaComposPipePiece(compute)))
        nextSteps.foreach{case (nextStep, str) => act ! (nextStep, str)}
        listOfSteps.get("inputL") match{
          case Some(mL: Map[String @ unchecked, _]) => listOfSteps.get("inputR") match{
            case Some(mR: Map[String @ unchecked, _]) =>
              build(mR, List(("nextStepR", act)), build(mL, List(("nextStepL", act)), firstSteps))
            case _ => firstSteps
          }
          case _ => firstSteps
        }
      case Some("ParallelPipes") =>
        listOfSteps.get("LeftPipe") match{
          case Some(mapL: Map[String @ unchecked, _]) => listOfSteps.get("RightPipe") match{
            case Some(mapR: Map[String @ unchecked, _]) => build(mapR, nextSteps, build(mapL, nextSteps, firstSteps))
            case _ => firstSteps
          }
          case _ => firstSteps
        }
      case Some("DataNode") =>
        listOfSteps.get("OutputMap") match{
          case Some(dataMap) =>
            val act = system.actorOf(Props(new AkkaPipePiece(NoJobAbstraction, dataMap)))
            nextSteps.foreach{
              case ("nextStep", a) => act ! ("nextStep", a); a ! ("init", dataMap)
              case ("nextStepL", a) => act ! ("nextStepL", a); a ! ("initL", dataMap)
              case ("nextStepR", a) => act ! ("nextStepR", a); a ! ("initR", dataMap)
              case ("nextStep?", a) => act ! ("nextStep", a)
              case _ => ()
            }
            ("", act) :: firstSteps
          case None =>
            nextSteps.foldRight(firstSteps){
              case (("nextStep", acRef), list) => ("P", acRef) :: list
              case (("nextStepL", acRef), list) => ("LP", acRef) :: list
              case (("nextStepR", acRef), list) => ("RP", acRef) :: list
              case (("nextStep?", acRef), list) => ("?", acRef) :: list
            }
        }
      case other =>
        println(s"ERROR: No such pipe piece as $other.")
        firstSteps
    }
  }
  override def run(l: List[(String, ActorRef)], s: String): Unit = l foreach{
    case (_, aRef) => aRef ! "output"
  }

}

class AkkaPipePiece[DMIn, DMOut](mTA: MThreadAbstraction, dataMapOut: DMOut) extends Actor{
  context.become(state())
  def state(dmIn: Option[DMIn] = None, nextSteps: List[(ActorRef, String)] = List()): Receive = {
    case "input" => if (!(mTA ! "input")) nextSteps.foreach{ case (aRef, _) => aRef ! "input" }
    case "output" => nextSteps.foreach{case (aRef, _) => aRef ! "input" }
    case ("nextStep", aRef: ActorRef) => context.become(state(dmIn, (aRef, "") :: nextSteps))
    case ("nextStepL", aRef: ActorRef) => context.become(state(dmIn, (aRef, "L") :: nextSteps))
    case ("nextStepR", aRef: ActorRef) => context.become(state(dmIn, (aRef, "R") :: nextSteps))
    case ("init") => context.become(state(None, nextSteps))
    case ("init", dataMapIn: DMIn @ unchecked) =>
      mTA ! ("init", dataMapIn, dataMapOut, self)
      context.become(state(Some(dataMapIn), nextSteps))
    case other => println(s"$other was sent without anything occurring.")
  }

  def receive: Receive = {
    case other => println(s"$other was sent here. How?")
  }
}

class AkkaComposPipePiece[DMInL, DMInR, DMOut](compute: (DMInL, DMInR, Option[DMOut]) => DMOut) extends Actor{
  context.become(state())
  def state(dmL: Option[DMInL] = None, dmR: Option[DMInR] = None,
            dMComb: Option[DMOut] = None, nextSteps: List[(ActorRef, String)] = List()): Receive = {
    case ("initL", dmL: DMInL @ unchecked) => context.become(state(Some(dmL), dmR, None, nextSteps))
    case ("initR", dmR: DMInR @ unchecked) => context.become(state(dmL, Some(dmR), None, nextSteps))
    case ("nextStep", aRef: ActorRef) => context.become(state(dmL, dmR, dMComb, (aRef, "") :: nextSteps))
    case ("nextStepL", aRef: ActorRef) => context.become(state(dmL, dmR, dMComb, (aRef, "L") :: nextSteps))
    case ("nextStepR", aRef: ActorRef) => context.become(state(dmL, dmR, dMComb, (aRef, "R") :: nextSteps))
    case "input" => (dmL, dmR) match{
      case (Some(dataMapL), Some(dataMapR)) =>
        val dMap = compute(dataMapL, dataMapR, dMComb)
        if (dMComb.isEmpty) {
          nextSteps.foreach {
            case (aRef, "") => aRef ! ("init", dMap)
            case (aRef, "L") => aRef ! ("initL", dMap)
            case (aRef, "R") => aRef ! ("initR", dMap)
            case _ => ()
          }
        }
        nextSteps.foreach { case (aRef, _) => aRef ! "input" }
        context.become(state(dmL, dmR, Some(dMap), nextSteps))
      case _ => ()
    }
    case other => println(s"$other was sent without anything occurring.")
  }
  def receive: Receive = {
    case other => println(s"$other somehow got to the incorrect state.")
  }
}
