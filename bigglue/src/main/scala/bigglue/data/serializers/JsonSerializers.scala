package bigglue.data.serializers

import bigglue.curators.Provenance
import bigglue.data.{BasicIdentity, Identifiable, Identity, VersionedIdentity}
import spray.json._

/**
  * Created by edmundlam on 8/21/17.
  */
/**
  * A simple serializer for flattened Json Identifiables.
  * @tparam Data The type of data that is to be serialized.
  *              NOTE: MUST BE [[Identifiable]].
  */
trait JsonSerializer[Data <: Identifiable[Data]] extends BasicSerializer[Data] {

  /**
    * This needs to be implemented by the serializer; This tells you how to serialize the object.
    * @param d The Data that needs to be serialized.
    * @return A JSON Representation of a (flattened) JSON object.
    */
  def serializeToJson_(d: Data): JsObject

  /**
    * This needs to be implemented by the serializer; This tells you how to deserialize the object.
    * @param json The JSON Representation of a (flattened) JSON object.
    * @return The unserialized Data.
    */
  def deserialize_(json: JsObject): Data

  /**
    * This does all of the serialization of data to a JSON Representation [[JsObject]].
    * This calls [[serializeToJson_]] for the actual serialization, but this takes care of the meta-data of the Identifiable.
    * @param d The Identifiable that needs to be serialized.
    * @return A JSON Representation of a (flattened) JSON Object.
    */
  def serializeToJson(d: Data): JsObject = {
    val jsObj = d.getVersion() match {
      case Some(version) => JsObject( serializeToJson_(d).fields + ("bg_version" -> JsString(version)) )
      case None => serializeToJson_(d)
    }
    /*JsObject(d.embedded.foldLeft(jsObj.fields){
      case (fields, (key, value)) => fields + (s"_embed_$key" -> JsString(value))
    })*/
    JsObject(d.getProvenance() match{
      case None => jsObj.fields
      case Some(prov) => jsObj.fields + ("_embed_provInfo" -> JsString(prov.id)) + ("_embed_provTime" -> JsString(prov.time))
    })
  }

  override def serialize_(d: Data): String = serializeToJson_(d).toString()

  /**
    * This takes care of the deserialization of the JSON.
    * First, it takes the serialized string and turns it into the [[JsObject]] representation of JSON.
    * Then, it takes care of the meta-data, and then calls [[deserialize_()]] on the rest of the data.
    * @param str The serialized version of the object.
    * @return The object that got rebuilt from the serialization.
    */
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
    (embed.get("provInfo"), embed.get("provTime")) match{
      case (Some(x), Some(y)) => dSerialized.setProvenance(Provenance(x, y))
      case _ => ()
    }
    //dSerialized.embedded = embed
    dSerialized
  }

}
