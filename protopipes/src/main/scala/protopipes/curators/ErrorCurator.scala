package protopipes.curators

import protopipes.data.{BasicIdentity, Identifiable, Identity}
import protopipes.store.DataMap
import protopipes.store.instances.InMemDataMap

/**
  * Created by edmundlam on 8/13/17.
  */
abstract class ErrorCurator[Input] {

  def reportError(src: Input, exception: Exception, extraInfo: Option[String] = None): Unit

}

class IdleErrorCurator[Input] extends ErrorCurator {

  override def reportError(src: Nothing, exception: Exception, extraInfo: Option[String]): Unit = { }

}

class StandardErrorCurator[Input <: Identifiable[Input]] extends ErrorCurator[Input] {

  val errorMap: DataMap[Identity[Input],String] = new InMemDataMap[Identity[Input], String]

  def errorMsg(exception: Exception, extraInfo: Option[String]): String = {
    exception.toString() ++ (extraInfo match {
      case Some(s) => " More info: " + s
      case None => "" })
  }

  override def reportError(src: Input, exception: Exception, extraInfo: Option[String]): Unit =
     errorMap.put(src.identity(), errorMsg(exception, extraInfo))

}