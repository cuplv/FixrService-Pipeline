package mthread_abstrac

/**
  * Created by chanceroberts on 7/27/17.
  */
class SingleThreadPipeline[A] extends MPipelineAbstraction[A] {
  override val isSingleThreaded = true
  override def build(listOfSteps: Map[String, Any], nextSteps: List[(String, A)] = List(), firstSteps: List[(String, A)] = List()): List[(String, A)] = {
    /*
    listOfSteps.get("PipeType") match {
      case Some("TransformationPipe") =>
        val mTA = listOfSteps.get("StepAbstraction") match {
          case Some(m: MThreadAbstraction[_, _, _, _]) => m
          case _ => return firstSteps //Please fix?
        }
        val dataMap = listOfSteps.get("OutputMap") match {
          case Some(x) => x
          case None => return firstSteps //Please fix?
        }
        (listOfSteps.get("input") match {
          case Some(m: Map[String @ unchecked, Any @ unchecked]) =>
            build(m, nextSteps, firstSteps)
          case _ => List()
        }) match {
          case (List(("inputMap", dMap: A))) => mTA ! ("init", dMap, dataMap); mTA ! "input"
        }

        ???
      case _ => ???
    }
    */
    println("Shouldn't ever get here yet! :\\")
    List()
  }


  override def run(l: List[(String, A)], s: String): Unit = {
    println("Shouldn't ever get here! :\\")
  }

  override def sendBack(message: Any, to: A, u: Unit): Unit = {
    println("Shouldn't ever get here yet! :\\")
  }
}
