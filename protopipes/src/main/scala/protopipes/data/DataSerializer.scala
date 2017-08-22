package protopipes.data

import java.util

import spray.json.{JsObject, JsString}

import spray.json._

/**
  * Created by edmundlam on 8/21/17.
  */

trait DataSerializer[A] {

  def serialize(a: A): String

  def deserialize(s: String): A

}

trait IdentityDataSerializer[A <: Identifiable[A]] extends DataSerializer[A] {

  def serialize_(a: A): String

  def deserialize_(s: String): A

  override def serialize(a: A): String = {
    val data = serialize_(a)
    a.identityOpt match {
      case Some(id) => JsObject("id" -> id.toJsonFormat(), "data" -> JsString(serialize_(a))).toString()
      case None => JsObject("data" -> JsString(serialize_(a))).toString()
    }
  }

  override def deserialize(s: String): A = {
    val json = s.parseJson.asJsObject
    val map = json.fields
    val a = deserialize_( map.get("data").get.toString() )
    if (map.contains("id")) {
      val idMap = map.get("id").get.asJsObject.fields
      val id = if (idMap.contains("version")) VersionedIdentity[A](idMap.get("id").get.toString(),idMap.get("version").get.toString())
               else BasicIdentity[A](idMap.get("id").get.toString)
      a.identityOpt = Some(id)
    }
    a
  }

}

object IStringSerializer extends DataSerializer[I[String]] {

  override def serialize(a: I[String]): String = a.i

  override def deserialize(s: String): I[String] = I(s)

}

object IdentityIStringSerializer extends IdentityDataSerializer[I[String]] {

  override def serialize_(a: I[String]): String = a.i

  override def deserialize_(s: String): I[String] = I(s)

}

object IIntSerializer extends DataSerializer[I[Int]] {

  override def serialize(a: I[Int]): String = s"${a.i}"

  override def deserialize(s: String): I[Int] = I(s.toInt)

}

object IdentityIIntSerializer extends IdentityDataSerializer[I[Int]] {

  override def serialize_(a: I[Int]): String = s"${a.i}"

  override def deserialize_(s: String): I[Int] = I(s.toInt)

}
