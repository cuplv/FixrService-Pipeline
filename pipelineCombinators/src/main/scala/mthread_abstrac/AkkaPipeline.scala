package mthread_abstrac

import akka.actor.{Actor, ActorRef, ActorSystem}

/**
  * Created by chanceroberts on 7/25/17.
  */
class AkkaPipeline(system: ActorSystem) extends MPipelineAbstraction {
  def build(listOfSteps: Map[String, Any]): List[(ActorRef, String)] = {
    val nextSteps = listOfSteps.getOrElse("nextSteps", List())
    val previousSteps = listOfSteps.getOrElse("previousSteps", List())
    ???
  }
}

class AkkaPipePiece[DMIn, DMOut](mTA: MThreadAbstraction, dataMapOut: DMOut) extends Actor{
  def state(dmIn: Option[DMIn] = None, nextSteps: List[(ActorRef, String)] = List()): Receive = {
    case "input" => mTA ! "input"
    case "output" => nextSteps.foreach{case (aRef, _) => aRef ! "input" }
    case ("nextStep", aRef: ActorRef) => context.become(state(dmIn, (aRef, "") :: nextSteps))
    case ("nextStepL", aRef: ActorRef) => context.become(state(dmIn, (aRef, "L") :: nextSteps))
    case ("nextStepR", aRef: ActorRef) => context.become(state(dmIn, (aRef, "R") :: nextSteps))
    case other => println(s"$other was sent without anything occurring.")
  }

  def receive: Receive = {
    case ("init") => context.become(state())
    case ("init", dataMapIn: DMIn) =>
      mTA ! ("init", dataMapIn, dataMapOut, self)
      context.become(state(Some(dataMapIn)))
    case other => println(s"$other was sent without anything occurring.")
  }
}

class AkkaComposPipePiece[DMInL, DMInR, DMOut](compute: (DMInL, DMInR, Option[DMOut]) => DMOut) extends Actor{
  context.become(state())
  def state(dmL: Option[DMInL] = None, dmR: Option[DMInR] = None,
            dMComb: Option[DMOut] = None, nextSteps: List[(ActorRef, String)] = List()): Receive = {
    case ("initL", dmL: DMInL) => context.become(state(Some(dmL), dmR))
    case ("initR", dmR: DMInR) => context.become(state(dmL, Some(dmR)))
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
