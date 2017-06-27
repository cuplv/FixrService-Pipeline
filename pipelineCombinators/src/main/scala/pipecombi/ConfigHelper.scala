package pipecombi
import com.typesafe.config.{Config, ConfigException}
/**
  * Created by chanceroberts on 6/27/17.
  */
object ConfigHelper {
  def possiblyInConfig(c: Config, field: String, default: String): String = {
    c match{
      case null => default
      case _ =>
        try {
          c.getString(field)
        } catch {
          case _: ConfigException.Missing => default
          case _: ConfigException.WrongType => default
        }
    }

  }

  def possiblyInConfig(c: Config, field: String, default: Boolean): Boolean = {
    c match {
      case null => default
      case _ =>
        try {
          c.getBoolean(field)
        } catch {
          case _: Exception => default
        }
    }
  }
}
