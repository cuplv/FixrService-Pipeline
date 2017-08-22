package protopipes.data.serializers

import protopipes.data._

import java.util

import spray.json.{JsObject, JsString}

import spray.json._

/**
  * Created by edmundlam on 8/21/17.
  */

trait StdSerializer[A] {

  def serialize(a: A): String

  def deserialize(s: String): A

}

trait IdentityStdSerializer[A <: Identifiable[A]] extends StdSerializer[A] {

  def serialize_(a: A): String

  def deserialize_(s: String): A

  override def serialize(a: A): String = {
    val data = serialize_(a)
    a.identityOpt match {
      case Some(id) => JsObject("identity" -> JsString(id.serialize()), "data" -> JsString(serialize_(a))).toString()
      case None => JsObject("data" -> JsString(serialize_(a))).toString()
    }
  }

  override def deserialize(s: String): A = {
    val json = s.parseJson.asJsObject
    val map = json.fields
    val a = deserialize_( map.get("data").get.toString().stripPrefix("\"").stripSuffix("\"") )
    if (map.contains("identity")) {
      /*
      val idMap = map.get("identity").get.asJsObject.fields
      val id = if (idMap.contains("version")) VersionedIdentity[A](idMap.get("id").get.toString(),idMap.get("version").get.toString())
               else BasicIdentity[A](idMap.get("id").get.toString) */
      a.identityOpt = Some( Identity.deserialize( map.get("identity").get.toString() ) )
    }
    // println(s"I got this: $json")
    a
  }

}

object IStringStdSerializer extends StdSerializer[I[String]] {

  override def serialize(a: I[String]): String = a.i

  override def deserialize(s: String): I[String] = I(s)

}

object IStringIdentityStdSerializer extends IdentityStdSerializer[I[String]] {

  override def serialize_(a: I[String]): String = a.i

  override def deserialize_(s: String): I[String] = I(s)

}

object IIntStdSerializer extends StdSerializer[I[Int]] {

  override def serialize(a: I[Int]): String = s"${a.i}"

  override def deserialize(s: String): I[Int] = I(s.toInt)

}

object IIntIdentityStdSerializer extends IdentityStdSerializer[I[Int]] {

  override def serialize_(a: I[Int]): String = s"${a.i}"

  override def deserialize_(s: String): I[Int] = I(s.toInt)

}

