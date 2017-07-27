package mthread_abstrac

import com.typesafe.config.Config

import scala.collection.JavaConverters._

/**
  * Created by chanceroberts on 6/27/17.
  */
object ConfigHelper {

  def possiblyInConfig(c: Option[Config], field: String, default: Option[Config]): Option[Config] = {
    c match{
      case None => default
      case Some(config) =>
        try {
          Some(config.getObject(field).toConfig)
        } catch {
          case _: Exception => default
        }
    }
  }

  def possiblyInConfig(c: Option[Config], field: String, default: List[String]): List[String] = {
    c match{
      case None => default
      case Some(config) =>
        try {
          config.getStringList(field).asScala.toList
        } catch{
          case _: Exception => default
          //case e: Exception => println(e); default
        }
    }
  }

  def possiblyInConfig(c: Option[Config], field: String, default: String): String = {
    c match{
      case None => default
      case Some(config) =>
        try {
          config.getString(field)
        } catch {
          case _: Exception => default
        }
    }
  }

  def possiblyInConfig(c: Option[Config], field: String, default: Int): Int = {
    c match{
      case None => default
      case Some(config) =>
        try {
          config.getInt(field)
        } catch{
          case _: Exception => default
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

}
