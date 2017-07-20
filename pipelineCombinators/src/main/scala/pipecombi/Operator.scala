package pipecombi

/**
  * Created by edmundlam on 6/23/17.
  */

abstract class Stat extends Identifiable

object Done extends Stat {
  override def identity(): Identity = Identity("Done", None)
}

object NotDone extends Stat {
  override def identity(): Identity = Identity("NotDone", None)
}
object Error extends Stat {
  override def identity(): Identity = Identity("Error", None)
}


abstract class ErrorSummary extends Identifiable

case class GeneralErrorSummary(ex: Exception) extends ErrorSummary  {
  override def identity(): Identity = Identity(s"GenError:${ex.toString}", None)
}

abstract class Operator[Arg1 <: Identifiable, Arg2 <: Identifiable, Output <: Identifiable] {
  def version: String

  val statMap: DataMap[Stat]
  val provMap: DataMap[Identity]
  val errMap:  DataMap[ErrorSummary]

  def operate(arg1: DataMap[Arg1], arg2: DataMap[Arg2]): DataMap[Output]
}