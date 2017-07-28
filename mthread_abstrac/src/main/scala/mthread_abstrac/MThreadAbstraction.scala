package mthread_abstrac

import com.typesafe.config.Config

/**
  * Created by chanceroberts on 7/24/17.
  */
abstract class MThreadAbstraction[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
                                                              succ: (Input, List[Output], DMOut) => DMOut, fail: (Input, Exception) => Unit,
                                                              config: Option[Config]) {
  var pipeline: Option[MPipelineAbstraction[_]] = None
  def send(message: Any): Boolean
  def !(message: Any): Boolean = send(message)
  def sendBack(message: Any): Boolean
  def !!(message: Any): Boolean = sendBack(message)
  def sendAPipeline(mPipelineAbstraction: MPipelineAbstraction[_]): Unit = { pipeline = Some(mPipelineAbstraction) }
}

abstract class Supervisor[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
                          succ: (Input, List[Output], DMOut) => DMOut, fail: (Input, Exception) => Unit){}

object MThreadBuilder {
  def build[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
            succ: (Input, List[Output], DMOut) => DMOut, fail: (Input, Exception) => Unit, config: Option[Config], default: String = ""): MThreadAbstraction[DMIn, DMOut, Input, Output] = {
    def firstInList(l: List[String]): String = l match{
      case Nil => default
      case field :: rest => ConfigHelper.possiblyInConfig(config, field, None) match{
        case None => firstInList(rest)
        case Some(_) => field
      }
    }
    firstInList(List("akka")) match{
      case "akka" => new AkkaAbstraction[DMIn, DMOut, Input, Output](getListOfInputs, compute, succ, fail, config)
      case _ => new SingleThreadAbstraction[DMIn, DMOut, Input, Output](getListOfInputs, compute, succ, fail, config)
    }
  }
}

object NoJobAbstraction extends MThreadAbstraction(null, null, null, null, None){
  override def send(message: Any): Boolean = message match{
    case "input" => false
    case _ => true
  }

  override def sendBack(message: Any): Boolean = false
}