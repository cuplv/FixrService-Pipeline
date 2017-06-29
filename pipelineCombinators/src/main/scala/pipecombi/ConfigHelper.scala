package pipecombi
import com.typesafe.config.{Config, ConfigException}
/**
  * Created by chanceroberts on 6/27/17.
  */
object ConfigHelper {
  def possiblyInConfig(c: Option[Config], field: String, default: String): String = {
    c match{
      case None => default
      case Some(config) =>
        try {
          config.getString(field)
        } catch {
          case _: ConfigException.Missing => default
          case _: ConfigException.WrongType => default
        }
    }

  }

  def possiblyInConfig(c: Option[Config], field: String, default: Boolean): Boolean = {
    c match {
      case None => default
      case Some(config) =>
        try {
          config.getBoolean(field)
        } catch {
          case _: Exception => default
        }
    }
  }

  def possiblyInConfig(c: Option[Config], field: String, default: Integer): Integer = {
    c match {
      case None => default
      case Some(config) =>
        try {
          config.getInt(field)
        } catch {
          case _: Exception => default
        }
    }
  }
}
