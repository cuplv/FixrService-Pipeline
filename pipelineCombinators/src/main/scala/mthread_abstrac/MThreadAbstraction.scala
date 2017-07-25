package mthread_abstrac

import com.typesafe.config.Config

/**
  * Created by chanceroberts on 7/24/17.
  */
abstract class MThreadAbstraction {
  def send(message: Any): Unit
}

abstract class Supervisor[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
                          succ: (Input, List[Output], DMOut) => DMOut, fail: (Input, Exception) => Unit){}

object MThreadBuilder {
  def build[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
            succ: (Input, List[Output], DMOut) => DMOut, fail: (Input, Exception) => Unit, config: Option[Config], default: String = ""): MThreadAbstraction = {
    def firstInList(l: List[String]): String = l match{
      case Nil => default
      case field :: rest => ConfigHelper.possiblyInConfig(config, field, None) match{
        case None => firstInList(rest)
        case Some(_) => field
      }
    }
    firstInList(List("akka")) match{
      case "akka" => new AkkaAbstraction[DMIn, DMOut, Input, Output](getListOfInputs, compute, succ, fail, config.get)
      case _ => ???
    }
  }
}