package protopipes.data.serializers

import protopipes.data._

import java.util

import spray.json.{JsObject, JsString}

import spray.json._

/**
  * Created by edmundlam on 8/21/17.
  */

trait BasicSerializer[A <: Identifiable[A]] {

  def serialize_(a: A): String

  def deserialize_(s: String): A

  def serialize(a: A): String = {
    a.getVersion() match {
      case Some(version) => JsObject("bg_version" -> JsString(version), "data" -> JsString(serialize_(a))).toString()
      case None          => JsObject("data" -> JsString(serialize_(a))).toString()
    }
  }

  def deserialize(s: String): A = {
    val json = s.parseJson.asJsObject
    val map = json.fields
    val a = deserialize_( map.get("data").get.toString().stripPrefix("\"").stripSuffix("\"").replace("\\\"", "\"") )
    if (map.contains("bg_version")) {
      a.setVersion( map.get("bg_version").get.toString().stripPrefix("\"").stripSuffix("\"").replace("\\\"", "\"") )
    }
    a
  }

}


object IStringBasicSerializer extends BasicSerializer[I[String]] {

  override def serialize_(a: I[String]): String = a.i

  override def deserialize_(s: String): I[String] = I(s)

}

object IIntBasicSerializer extends BasicSerializer[I[Int]] {

  override def serialize_(a: I[Int]): String = s"${a.i}"

  override def deserialize_(s: String): I[Int] = I(s.toInt)

}

