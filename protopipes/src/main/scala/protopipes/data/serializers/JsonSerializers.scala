package protopipes.data.serializers

import protopipes.data.{BasicIdentity, Identifiable, Identity, VersionedIdentity}
import spray.json._

/**
  * Created by edmundlam on 8/21/17.
  */
trait JsonSerializer[Data] {

  def serialize(d: Data): String

  def deserialize(json: JsObject): Data

}

trait IdentityJsonSerializer[Data <: Identifiable[Data]] extends JsonSerializer[Data] {

  def serialize_(d: Data): JsObject

  def deserialize_(json: JsObject): Data

  override def serialize(d: Data): String = {
    val dataJson = serialize_(d)
    d.identityOpt match {
      case Some(id) => JsObject(dataJson.fields + ("identity" -> JsString(id.serialize()))).toString()
      case None => dataJson.toString
    }
  }

  override def deserialize(json: JsObject): Data = {
    val data = deserialize_(json)
    val map = json.fields
    if (map.contains("identity")) {
      /*
      val idMap = map.get("identity").get.asJsObject.fields
      val id = if (idMap.contains("version")) VersionedIdentity[Data](idMap.get("id").get.toString(), idMap.get("version").get.toString())
               else BasicIdentity[Data](idMap.get("id").get.toString) */
      data.identityOpt = Some( Identity.deserialize( map.get("identity").get.toString() ) )
    }
    println(s"I got this: $json")
    data
  }

  def deserialize(str: String): Data = deserialize( str.parseJson.asJsObject )

}
