package bigglue.data.serializers

import bigglue.data.{BasicIdentity, Identifiable, Identity, VersionedIdentity}
import spray.json._

/**
  * Created by edmundlam on 8/21/17.
  */
trait JsonSerializer[Data <: Identifiable[Data]] extends BasicSerializer[Data] {

  def serializeToJson_(d: Data): JsObject

  def deserialize_(json: JsObject): Data

  def serializeToJson(d: Data): JsObject = {
    val jsObj = d.getVersion() match {
      case Some(version) => JsObject( serializeToJson_(d).fields + ("bg_version" -> JsString(version)) )
      case None => serializeToJson_(d)
    }
    JsObject(d.embedded.foldLeft(jsObj.fields){
      case (fields, (key, value)) => fields + (s"_embed_$key" -> JsString(value))
    })
  }

  override def serialize_(d: Data): String = serializeToJson_(d).toString()

  override def deserialize_(str: String): Data = {
    val obj = str.parseJson.asJsObject
    val (newFields, embed) = obj.fields.foldLeft(Map[String, JsValue](), Map[String, String]()){
      case ((nFields, emb), (key, value)) => value match{
        case JsString(j) => key.length match{
          case x if x > 7 && key.substring(0,7).equals("_embed_") => (nFields, emb+(key.substring(7)->j))
          case _ => (nFields+(key->value), emb)
        }
        case _ => (nFields+(key->value), emb)
      }
    }
    val newObj = JsObject(fields=newFields)
    val dSerialized = deserialize_( obj )
    dSerialized.embedded = embed
    dSerialized
  }

}
