package protopipes.store.instances

import protopipes.data.Identifiable
import protopipes.data.serializers.JsonSerializer
import protopipes.exceptions.CallNotAllowException
import protopipes.store.{DataMap, DataMultiMap, DataStore}
import spray.json._

import scala.util.Random
import scala.util.parsing.json.JSON
import scalaj.http.Http


/**
  * Created by chanceroberts on 8/10/17.
  */

case class SolrClassObject(name: String, args: List[Any])

object SolrInstances{
  def createIdDataMap[Data <: Identifiable[Data]](name: String): DataStore[Data] = {
    ???
  }
}

trait SolrSpecific[Key, Data <: Identifiable[Data]] extends JsonSerializer[Data]{
  var nam = ""
  var url = "http://localhost:8983/solr/gettingstarted/"
  def init(nm: String, URL: String): Unit = {
    nam = nm
    url = URL
  }

  def keyToString(key: Key): String = {
    key match {
      case i: Identifiable[_] => i.getId().toString
      case _ => key.toString
    }
  }

  //def toJson(d: Data): String

  //def fromJson(s: String): Data

  def getDocument(id: String): Option[JsObject] = {
    def getDocumentFromList(list: List[JsObject], id: String): Option[JsObject] = list match{
      case Nil => None
      case first :: rest => first.fields.get("id") match{
        case Some(x: JsString) if x.value.equals(id) => Some(first)
        case _ => getDocumentFromList(rest, id)
      }
    }
    val queryURL = url+"select?wt=json&q=id=\"" + id + "\""
    val json = Http(queryURL).asString.body
    json.parseJson.asJsObject().fields.get("response") match{
      case Some(response: JsValue) => response.asJsObject.fields.get("docs")  match{
        case Some(docs: JsArray) => getDocumentFromList(docs.elements.toList.map(_.asJsObject), id)
      }
    }
  }

  def getDocuments(id: String, field: String = "_id_", rows: String = "1000000000"): List[JsObject] = {
    val queryURL = url+"select?wt=json&rows=" + rows + "&q="+field+"=\""+ id + "\""
    val json = Http(queryURL).asString.body
    json.parseJson.asJsObject.fields.get("response") match{
      case Some(response: JsValue) => response.asJsObject.fields.get("docs") match{
        case Some(docs: JsArray) => docs.elements.foldRight(List[JsObject]()){
          case (v: JsValue, list) =>
            val obj = v.asJsObject
            obj.fields.get(field) match{
              case Some(str: JsString) if str.value.equals(id) => obj :: list
              case _ => list
            }
        }
        case _ => List()
      }
      case _ => List()
    }
  }

  def addDocument(doc: JsObject, key: Key): Unit = {
    val json = JsObject(Map("add" -> JsObject(Map("doc" -> doc)), "commit" -> JsObject())).toString()
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    result.parseJson.asJsObject.fields.get("error") match{
      case Some(x) => throw new Exception(s"Could not update the DataMap $nam on key(s) $key.")
      case _ => ()
    }
  }

  def deleteDocuments(ids: List[String]): Unit = {
    val json = JsObject(Map("delete" -> JsArray(ids.map(JsString(_)).toVector))).toString
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    result.parseJson.asJsObject.fields.get("error") match{
      case Some(x) => throw new Exception(s"Could not update the DataMap $nam on ID(s) $ids.")
      case _ => ()
    }
  }

  def getAllDocuments: List[JsObject] = {
    val queryURL = url+"select?&rows=100000000&q=*:*"
    val json = Http(queryURL).asString.body
    json.parseJson.asJsObject.fields.get("response") match {
      case Some(response: JsValue) => response.asJsObject.fields.get("docs") match{
        case Some(docs: JsArray) => docs.elements.toList.map(_.asJsObject)
        case _ => List()
      }
    }
  }
}

abstract class SolrDataMap[Key, Data <: Identifiable[Data]](coreName: String, solrLocation: String = "localhost:8983", needToCreate: Boolean = false) extends DataMap[Key, Data] with SolrSpecific[Key, Data] {
  if (needToCreate) {
    Http(s"http://$solrLocation/solr/admin/collections?action=CREATE&name=$coreName&numShards=1")
  }
  init(name, s"http://$solrLocation/solr/$coreName/")

  override def put_(key: Key, data: Data): Unit = {
    val kToString = keyToString(key)
    val document = serializeToJson(data)
    val doc = JsObject(document.fields + ("id" -> JsString(data.getId())))
    addDocument(doc, key)
  }

  override def put_(data: Seq[Data]): Unit = {}

  override def get(key: Key): Option[Data] = {
    getDocument(keyToString(key)) match{
      case Some(jsObject: JsObject) =>
        Some(deserialize(JsObject(jsObject.fields -- List("id", "_version_")).toString))
      case _ => None
    }
  }

  override def contains(key: Key): Boolean = getDocument(keyToString(key)) match {
    case Some(_) => true
    case _ => false
  }

  override def all(): Seq[Data] = {
    getAllDocuments.foldRight(List[Data]()) {
      case (jsObj, data) =>
        val jsonable = JsObject(jsObj.fields -- List("id", "_version_")).toString
        deserialize(jsonable) :: data
    }
  }

  override def remove(key: Key): Unit = {
    deleteDocuments(List(keyToString(key)))
  }

  override def remove(keys: Seq[Key]): Unit = {
    deleteDocuments(keys.foldRight(List.empty[String]){
      case (key, list) => keyToString(key) :: list
    })
  }

  override def extract(): Seq[Data] = {
    val toReturn = all()
    Http(url+"update").postData("{ \"delete\": { \"query\":\"*:*\" }, \"commit\": {}").header("Content-Type", "application/json")
    toReturn
  }

  override def size(): Int = {
    val queryURL = url+"select?wt=json&q=*:*"
    val json = Http(queryURL).asString.body
    json.parseJson.asJsObject.fields.get("response") match {
      case Some(response: JsValue) => response.asJsObject.fields.get("numFound") match{
        case Some(num: JsNumber) => num.value.intValue()
        case Some(str: JsString) => str.value.toInt
        case _ => 0
      }
      case _ => 0
    }
  }

  override def iterator(): Iterator[Data] = ???
}

abstract class SolrMultiMap[Key, Data <: Identifiable[Data]](coreName: String, solrLocation: String = "localhost:8983", needToCreate: Boolean = false) extends DataMultiMap[Key, Data] with SolrSpecific[Key, Data] {
  if (needToCreate) {
    Http(s"http://$solrLocation/solr/admin/collections?action=CREATE&name=$coreName&numShards=1")
  }
  init(name, s"http://$solrLocation/solr/$coreName/")

  override def put_(data: Seq[Set[Data]]): Unit = {}

  override def put_(key: Key, data: Set[Data]): Unit = {
    val kToString = keyToString(key)
    data.foreach{dta =>
      val document = serializeToJson(dta)
      val doc = JsObject(document.fields +
        ("_id_" -> JsString(dta.getId()), "id" -> JsString(s"${kToString}___${dta.getId()}_${Random.nextInt(99999)}")))
      addDocument(doc, key)
    }
  }

  override def remove(keys: Seq[Key]): Unit = {
    keys.foreach(key => remove(key))
  }

  override def remove(key: Key): Unit = {
    deleteDocuments(getDocuments(keyToString(key)).foldRight(List[String]()) {
      case (jsObj, list) => jsObj.fields.get("id") match{
        case Some(s: JsString) => s.value :: list
        case _ => list
      }
    })
  }

  override def remove(key: Key, data: Set[Data]): Unit = {
    val serializedMap = data.foldRight(Map[String, Unit]()) {
      case (dta, map) => map + (serialize(dta) -> ())
    }
    deleteDocuments(getDocuments(keyToString(key)).foldRight(List[String]()){
      case (jsObj, toRemove) =>
        serializedMap.get(JsObject(jsObj.fields -- List("id", "_version_", "_id_")).toString()) match {
          case Some(_) => jsObj.fields.get("id") match{
            case Some(id: JsString) => id.value :: toRemove
            case _ => toRemove
          }
          case _ => toRemove
        }
    })
  }

  override def remove(data: Set[Data]): Unit = {
    val serializedMap = data.foldRight(Map[String, Unit]()) {
      case (dta, map) => map + (serialize(dta) -> ())
    }
    deleteDocuments(getAllDocuments.foldRight(List[String]()){
      case (jsObj, toRemove) =>
        serializedMap.get(JsObject(jsObj.fields -- List("id", "_version_", "_id_")).toString()) match {
          case Some(_) => jsObj.fields.get("id") match{
            case Some(id: JsString) => id.value :: toRemove
            case _ => toRemove
          }
          case _ => toRemove
        }
    })
  }

  override def iterator(): Iterator[Set[Data]] = throw new CallNotAllowException(???, None)

  override def iterator(key: Key): Iterator[Data] = ???

  override def get(key: Key): Set[Data] = {
    getDocuments(keyToString(key)).foldRight(Set[Data]()) {
      case (jsObj, set) => set + deserialize(jsObj.toString)
    }
  }

  override def all(): Seq[Set[Data]] = {
    getAllDocuments.foldRight(Map[JsString, Set[Data]]()) {
      case (jsObj, data) =>
        jsObj.fields.get("_id_") match{
          case Some(id: JsString) => data.get(id) match {
            case Some(set) => data + (id -> (set + deserialize(jsObj.toString)))
            case None => data + (id -> Set(deserialize(jsObj.toString)))
          }
          case _ => data
        }
    }.toList.unzip._2
  }

  override def contains(key: Key, data: Set[Data]): Boolean = data subsetOf get(key)

  override def extract(): Seq[Set[Data]] = {
    val mapOfSets = all()
    Http(url+"update").postData("{ \"delete\": { \"query\":\"*:*\" }, \"commit\": {}").header("Content-Type", "application/json")
    mapOfSets
  }

  override def size(): Int = {
    getAllDocuments.foldRight(Map[JsString, Unit]()) {
      case (jsObj, keysFound) =>
        jsObj.fields.get("_id_") match{
          case Some(id: JsString) => keysFound.get(id) match {
            case Some(set) => keysFound
            case None => keysFound + (id -> ())
          }
          case _ => keysFound
        }
    }.size
  }
}

class SolrIterator[Data] extends Iterator[Data] {

  override def next(): Data = ???

  override def hasNext: Boolean = ???

}