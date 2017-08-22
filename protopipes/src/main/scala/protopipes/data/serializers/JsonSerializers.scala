package protopipes.data.serializers

import protopipes.data.{BasicIdentity, Identifiable, Identity, VersionedIdentity}
import spray.json._

/**
  * Created by edmundlam on 8/21/17.
  */
trait JsonSerializer[Data <: Identifiable[Data]] extends BasicSerializer[Data] {

  def serializeToJson_(d: Data): JsObject

  def deserialize_(json: JsObject): Data

  def serializeToJson(d: Data): JsObject = {
    d.getVersion() match {
      case Some(version) => JsObject( serializeToJson_(d).fields + ("bg_version" -> JsString(version)) )
      case None => serializeToJson_(d)
    }
  }

  override def serialize_(d: Data): String = serializeToJson_(d).toString()

  override def deserialize_(str: String): Data = deserialize_( str.parseJson.asJsObject )

}
