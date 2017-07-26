package mthread_abstrac

import com.typesafe.config.Config

/**
  * Created by chanceroberts on 7/25/17.
  */
abstract class MPipelineAbstraction[A] {
  def build(listOfSteps: Map[String, Any], nextSteps: List[(String, A)] = List(), firstSteps: List[(String, A)] = List()): List[(String, A)]
  def build(listOfSteps: Map[String, Any]): List[(String, Any)] = build(listOfSteps, List(), List()).asInstanceOf[List[(String, Any)]]
  def run(l: List[(String, A)], s: String): Unit
  def run(l: List[(String, Any)]): Unit = run(l.asInstanceOf[List[(String, A)]], "run")
}

object MPipelineBuilder {
  def build(conf: Option[Config]): MPipelineAbstraction[_] = {
    def firstInList(l: List[String], default: String): String = l match{
      case Nil => default
      case field :: rest => ConfigHelper.possiblyInConfig(conf, field, None) match{
        case None => firstInList(rest, default)
        case Some(_) => field
      }
    }
    firstInList(List("akka"), "akka") match{
      case "akka" => new AkkaPipeline(AkkaPipelineBuilder.getSystem)
      case _ => ???
    }
  }
}
