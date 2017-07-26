package mthread_abstrac

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import scala.concurrent.duration._

/**
  * Created by chanceroberts on 7/25/17.
  */
class AkkaPipeline(system: ActorSystem) extends MPipelineAbstraction {
  def build(listOfSteps: Map[String, Any], nextSteps: List[(String, ActorRef)] = List(), firstSteps: List[(String, ActorRef)] = List()): List[(String, ActorRef)] = {
    listOfSteps.get("pipeType") match {
      case Some("JunctionPipe") =>
        val l: List[(String, ActorRef)] = listOfSteps.get("OutputPipes") match{
          case Some(m: Map[String, Any]) => build(m, List(), List())
          case None => List()
        }
        val (nextSteps, newFirstSteps) = l.foldRight((List()[(String, ActorRef)], firstSteps)){
          case (("" | "?", aRef), (next, first)) => (next, ("?", aRef) :: first)
          case (("P", aRef), (next, first)) => (("nextStep", aRef) :: next, first)
          case (("LP", aRef), (next, first)) => (("nextStepL", aRef) :: next, first)
          case (("RP", aRef), (next, first)) => (("nextStepR", aRef) :: next, first)
        }
        listOfSteps.get("input") match{
          case Some(m: Map[String, Any]) => build(m, nextSteps, newFirstSteps)
          case None => newFirstSteps
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
        /*firstSteps.foreach{
          case ("?", a) =>

            ???
        }*/
        nextSteps.foreach{
          case ("nextStep", a) => act ! ("nextStep", a); a ! ("init", dataMap)
          case ("nextStepL", a) => act ! ("nextStepL", a); a ! ("initL", dataMap)
          case ("nextStepR", a) => act ! ("nextStepR", a); a ! ("initR", dataMap)
        }
        listOfSteps.get("input") match{
          case Some(m: Map[String, Any]) => build(m, List(("nextStep", act)), firstSteps)
          case None => firstSteps
        }
      case Some("CompositionPipe") =>
        val compute = listOfSteps.get("composer") match{
          case Some(a: ((Any, Any, Option[Any]) => Any)) => a
          case _ => return firstSteps //Please fix?
        }
        val act = system.actorOf(Props(new AkkaComposPipePiece(compute)))
        nextSteps.foreach{act ! _}
        listOfSteps.get("inputL") match{
          case Some(mL: Map[String, Any]) => listOfSteps.get("inputR") match{
            case Some(mR: Map[String, Any]) =>
              build(mR, List(("nextStepR", act)), build(mL, List(("nextStepL", act)), firstSteps))
            case _ => firstSteps
          }
          case _ => firstSteps
        }
      case Some("ParallelPipe") =>
        listOfSteps.get("LeftPipe") match{
          case Some(mapL: Map[String, Any]) => listOfSteps.get("RightPipe") match{
            case Some(mapR: Map[String, Any]) => build(mapR, nextSteps, build(mapL, nextSteps, firstSteps))
            case None => firstSteps
          }
          case None => firstSteps
        }
      case Some("DataNode") =>
        listOfSteps.get("OutputMap") match{
          case Some(dataMap) =>
            val act = system.actorOf(Props(new AkkaPipePiece(NoJobAbstraction, dataMap)))
            nextSteps.foreach{
              case ("nextStep", a) => act ! ("nextStep", a); a ! ("init", dataMap)
              case ("nextStepL", a) => act ! ("nextStepL", a); a ! ("initL", dataMap)
              case ("nextStepR", a) => act ! ("nextStepR", a); a ! ("initR", dataMap)
            }
            ("", act) :: firstSteps
          case None =>
            nextSteps.foldRight(firstSteps){
              case (("nextStep", acRef), list) => ("P", acRef) :: list
              case (("nextStepL", acRef), list) => ("LP", acRef) :: list
              case (("nextStepR", acRef), list) => ("RP", acRef) :: list
            }
        }
      case other =>
        println(s"ERROR: No such pipe piece as $other.")
        firstSteps
    }
  }
}

class AkkaPipePiece[DMIn, DMOut](mTA: MThreadAbstraction, dataMapOut: DMOut) extends Actor{
  def state(dmIn: Option[DMIn] = None, nextSteps: List[(ActorRef, String)] = List()): Receive = {
    case "input" => mTA ! "input"
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
    case ("init") => context.become(state())
    case ("init", dataMapIn: DMIn @ unchecked) =>
      mTA ! ("init", dataMapIn, dataMapOut, self)
      context.become(state(Some(dataMapIn)))
    case other => println(s"$other was sent without anything occurring.")
  }
}

class AkkaComposPipePiece[DMInL, DMInR, DMOut](compute: (DMInL, DMInR, Option[DMOut]) => DMOut) extends Actor{
  context.become(state())
  def state(dmL: Option[DMInL] = None, dmR: Option[DMInR] = None,
            dMComb: Option[DMOut] = None, nextSteps: List[(ActorRef, String)] = List()): Receive = {
    case ("initL", dmL: DMInL @ unchecked) => context.become(state(Some(dmL), dmR))
    case ("initR", dmR: DMInR @ unchecked) => context.become(state(dmL, Some(dmR)))
    case ("nextStep", aRef: ActorRef) => context.become(state(dmL, dmR, dMComb, (aRef, "") :: nextSteps))
    case ("nextStepL", aRef: ActorRef) => context.become(state(dmL, dmR, dMComb, (aRef, "L") :: nextSteps))
    case ("nextStepR", aRef: ActorRef) => context.become(state(dmL, dmR, dMComb, (aRef, "R") :: nextSteps))
    case "input" => (dmL, dmR) match{
      case (Some(dataMapL), Some(dataMapR)) =>
        val dMap = compute(dataMapL, dataMapR, dMComb)
        context.become(state(dmL, dmR, Some(dMap), nextSteps))
      case _ => ()
    }
    case other => println(s"$other was sent without anything occurring.")
  }
  def receive: Receive = {
    case other => println(s"$other somehow got to the incorrect state.")
  }
}
