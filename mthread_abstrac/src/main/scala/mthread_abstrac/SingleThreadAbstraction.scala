package mthread_abstrac

import com.typesafe.config.Config

/**
  * Created by chanceroberts on 7/27/17.
  */
class SingleThreadAbstraction[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
                              succ: (Input, List[Output], DMOut) => DMOut, fail: (Input, Exception) => Unit, config: Option[Config]) extends MThreadAbstraction[DMIn, DMOut, Input, Output](getListOfInputs, compute, succ, fail, config) {
  var dataMapIn: Option[DMIn] = None
  var dataMapOut: Option[DMOut] = None
  override def send(message: Any): Boolean = {
    message match {
      case ("init", dMI: DMIn @ unchecked, dMO: DMOut @ unchecked, _) =>
        dataMapIn = Some(dMI)
        dataMapOut = Some(dMO)
      case "input" => (dataMapIn, dataMapOut) match {
        case (Some(dIn), Some(dOut)) =>
          val listOfInputs = getListOfInputs(dIn)
          listOfInputs.foreach(input => succ(input, compute(input), dOut))
        case _ => ()
      }
      case other => println(s"$other was sent with nothing occurring.")
    }
    false
  }

  override def sendBack(message: Any): Boolean = true
}