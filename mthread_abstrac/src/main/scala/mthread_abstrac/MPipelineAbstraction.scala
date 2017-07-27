package mthread_abstrac

import com.typesafe.config.Config

/**
  * Created by chanceroberts on 7/25/17.
  */
abstract class MPipelineAbstraction[A] {
  def isSingleThreaded = false
  def build(listOfSteps: Map[String, Any], nextSteps: List[(String, A)] = List(), firstSteps: List[(String, A)] = List()): List[(String, A)]
  def build(listOfSteps: Map[String, Any]): List[(String, Any)] = build(listOfSteps, List(), List()).foldRight(List.empty[(String, Any)]){
    case ((str, a), list) => (str, a) :: list
  }
  def run(l: List[(String, A)], s: String): Unit
  def run(l: List[(String, Any)]): Unit = run(l.asInstanceOf[List[(String, A)]], "run")
}

object MPipelineBuilder {
  def build(conf: Option[Config], default: String = ""): MPipelineAbstraction[_] = {
    def firstInList(l: List[String], default: String): String = l match{
      case Nil => default
      case field :: rest => ConfigHelper.possiblyInConfig(conf, field, None) match{
        case None => firstInList(rest, default)
        case Some(_) => field
      }
    }
    firstInList(List("akka"), default) match{
      case "akka" => new AkkaPipeline(AkkaPipelineBuilder.getSystem)
      //Right now the default is the Akka since I have only implemented that.
      //Later, I may change it to a Single Threaded system or something.
      case _ => new AkkaPipeline(AkkaPipelineBuilder.getSystem)
    }
  }
}
