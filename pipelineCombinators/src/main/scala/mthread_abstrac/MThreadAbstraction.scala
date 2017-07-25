package mthread_abstrac

import akka.actor.Actor

/**
  * Created by chanceroberts on 7/24/17.
  */
abstract class MThreadAbstraction {
  def send(message: Any): Unit
}

abstract class Supervisor[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
                          succ: (Input, List[Output]) => Any, fail: (Input, Exception) => Any){}
