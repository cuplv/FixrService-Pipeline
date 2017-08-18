package protopipes.exceptions

/**
  * Created by edmundlam on 8/17/17.
  */

object ProtoPipeException {

  def defaultMsg(message: Option[String], cause: Option[Throwable]): String = {
    val m = message match {
      case Some(m) => m
      case None => ""
    }
    val c = cause match {
        case Some(t) => "Cause by: " + t.toString
        case None => ""
    }
    m + c
  }

}

class ProtoPipeException(message: Option[String] = None, cause: Option[Throwable] = None)
    extends RuntimeException(ProtoPipeException.defaultMsg(message, cause), cause.getOrElse(null))

class UserComputationException(compName: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Problem in user computation: $compName"), cause = cause)

class NotInitializedException(component: String, operation: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"$operation not allowed: $component has not been initialized"), cause = cause)

class UnexpectedPipelineException(context: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Problem in pipeline: $context"), cause = cause)

class MalformedPipelineConfigException(context: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Pipeline configuration is malformed: $context"), cause = cause)

class IncompatiblePipelineSegmentException(context: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Pipeline components are incompatible: $context"), cause = cause)

class CallNotAllowException(msg: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(msg), cause = cause)