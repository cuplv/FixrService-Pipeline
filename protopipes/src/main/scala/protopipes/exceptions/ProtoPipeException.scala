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

/**
  *
  * The generic exception class for ProtoPipe exceptions
  *
  * @param message description of the exception
  * @param cause an associated throwable
  */
class ProtoPipeException(message: Option[String] = None, cause: Option[Throwable] = None)
    extends RuntimeException(ProtoPipeException.defaultMsg(message, cause), cause.getOrElse(null))

/**
  * This is thrown when an uncaught (by user) exception occurred in a user-defined computation
  *
  * @param compName the name of the computation
  * @param cause an associated throwable
  */
class UserComputationException(compName: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Problem in user computation: $compName"), cause = cause)

/**
  * This is thrown when the runtime attempts to call a pipeline operation that requires initialization, but the
  * pipeline has not been initialized.
  *
  * @param component the name of the pipeline component that received this call
  * @param operation the name of the operation
  * @param cause an associated throwable
  */
class NotInitializedException(component: String, operation: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"$operation not allowed: $component has not been initialized"), cause = cause)

/**
  * This is thrown at top-level pipeline try catch blocks. It means that some bad and unexpected has happened in the runtime.
  *
  * @param context description of the location where this is thrown
  * @param cause an associated throwable
  */
class UnexpectedPipelineException(context: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Problem in pipeline: $context"), cause = cause)

/**
  * This is thrown when a configuration file checking routine has determined a problem in a configuration path
  *
  * @param context description of the problem.
  * @param cause an associated throwable
  */
class MalformedPipelineConfigException(context: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Pipeline configuration is malformed: $context"), cause = cause)

/**
  * This is thrown when a configuration type supplied to a specific segment of the pipeline is not supported by the
  * components of that segment. E.g., supplying Java utils properties configuration to Akka actor platform.
  *
  * @param context description of the context.
  * @param cause an associated throwable
  */
class NotSupportedPipelineConfigException(context: String, cause: Option[Throwable])
  extends ProtoPipeException(message = Some(s"Pipeline configuration type is not supported: $context"), cause = cause)

/**
  * This is thrown when an input/output DataStore checking routine has determined an incompatibility with the associated
  * computation/platform
  *
  * @param context description of the incompatibility.
  * @param cause an associated throwable
  */
class IncompatiblePipelineSegmentException(context: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Pipeline components are incompatible: $context"), cause = cause)

/**
  * This is generally thrown when a method was called on a receiver instance that does not handle that call
  *
  * @param msg description of receiver and method call.
  * @param cause an associated throwable
  */
class CallNotAllowException(msg: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(msg), cause = cause)

/**
  * This is generally thrown when a config loading routine encountered path patterns that are not supported by
  * the current implementation
  *
  * @param context description of the location (in the config file) of the unsupported config path.
  * @param cause an associated throwable
  */
class NotSupportedConfigFormatException(context: String, cause: Option[Throwable])
    extends ProtoPipeException(message = Some(s"Configuration path formatting not supported: $context"), cause = cause)