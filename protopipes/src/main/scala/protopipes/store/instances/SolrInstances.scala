package protopipes.store.instances

import protopipes.data.Identifiable
import protopipes.data.serializers.JsonSerializer
import protopipes.exceptions.{CallNotAllowException, NotInitializedException, ProtoPipeException}
import protopipes.store.{DataMap, DataMultiMap, DataStore}
import spray.json._

import scala.util.Random
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

trait SolrSpecific[Data <: Identifiable[Data]]{
  var nam = ""
  var url = "http://localhost:8983/solr/gettingstarted/"
  def init(nm: String, URL: String): Unit = {
    nam = nm
    url = URL
  }

  def keyToString(key: Any): String = {
    key match {
      case i: Identifiable[_] => i.getId().toString
      case _ => key.toString
    }
  }

  def getDocumentsFromQuery(query: String): List[JsObject] = {
    val queryURL = url+query
    val json = Http(queryURL).asString.body
    json.parseJson.asJsObject().fields.get("response") match {
      case Some(response: JsValue) => response.asJsObject.fields.get("docs") match {
        case Some(docs: JsArray) => docs.elements.toList.map(_.asJsObject)
        case _ =>
          throw new ProtoPipeException(Some(s"SolrMap $nam response is malformed. Are you sure you're really using Solr?"), None)
      }
      case _ =>
        throw new ProtoPipeException(Some(s"Probably got an error in SolrMap $nam on query $query. Please fix your query."), None)
    }
  }

  def getDocument(id: String): Option[JsObject] = {
    def getDocumentFromList(list: List[JsObject], id: String): Option[JsObject] = list match{
      case Nil => None
      case first :: rest => first.fields.get("id") match{
        case Some(x: JsString) if x.value.equals(id) => Some(first)
        case _ => getDocumentFromList(rest, id)
      }
    }
    val docsToCheck = getDocumentsFromQuery("select?wt=json&q=id=\"" + id + "\"")
    getDocumentFromList(docsToCheck, id)
  }

  def getDocuments(id: String, field: String = "_id_"): List[JsObject] = {
    val docsToCheck = getDocumentsFromQuery("select?wt=json&rows=1000000000&q="+field+"=\""+ id + "\"")
    docsToCheck.foldRight(List[JsObject]()) {
      case (v: JsValue, list) =>
        val obj = v.asJsObject()
        obj.fields.get(field) match {
          case Some(str: JsString) if str.value.equals(id) => obj :: list
          case _ => list
        }
    }
  }

  def addDocument(doc: JsObject, key: String): Unit = {
    val json = JsObject(Map("add" -> JsObject(Map("doc" -> doc)), "commit" -> JsObject())).toString()
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    result.parseJson.asJsObject.fields.get("error") match{
      case Some(x) => throw new Exception(s"Could not update the core $nam on key $key.")
      case _ => ()
    }
  }

  def deleteDocuments(ids: List[String]): Unit = {
    val json = JsObject(Map("delete" -> JsArray(ids.map(JsString(_)).toVector))).toString
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    result.parseJson.asJsObject.fields.get("error") match{
      case Some(x) => throw new Exception(s"Could not update the core $nam on ID(s) $ids.")
      case _ => ()
    }
  }

  def getAllDocuments: List[JsObject] = getDocumentsFromQuery("select?&rows=100000000&q=*:*")
}

abstract class SolrDataMap[Key, Data <: Identifiable[Data]](serializer: JsonSerializer[Data], coreName: String, solrLocation: String = "localhost:8983", needToCreate: Boolean = false) extends DataMap[Key, Data] with SolrSpecific[Data] {
  if (needToCreate) {
    Http(s"http://$solrLocation/solr/admin/collections?action=CREATE&name=$coreName&numShards=1")
  }
  init(coreName, s"http://$solrLocation/solr/$coreName/")

  override def put_(key: Key, data: Data): Unit = {
    val kToString = keyToString(key)
    val document = serializer.serializeToJson(data)
    val doc = JsObject(document.fields + ("id" -> JsString(kToString)))
    addDocument(doc, kToString)
  }

  override def put_(data: Seq[Data]): Unit = {}

  override def get(key: Key): Option[Data] = {
    getDocument(keyToString(key)) match{
      case Some(jsObject: JsObject) =>
        Some(serializer.deserialize_(JsObject(jsObject.fields -- List("id", "_version_")).toString))
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
        serializer.deserialize_(jsonable) :: data
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

  override def iterator(): Iterator[Data] = new SolrIterator[Data](serializer, coreName, solrLocation)
}

abstract class SolrMultiMap[Key, Data <: Identifiable[Data]](serializer: JsonSerializer[Data], coreName: String, solrLocation: String = "localhost:8983", needToCreate: Boolean = false) extends DataMultiMap[Key, Data] with SolrSpecific[Data] {
  if (needToCreate) {
    Http(s"http://$solrLocation/solr/admin/collections?action=CREATE&name=$coreName&numShards=1")
  }
  init(name, s"http://$solrLocation/solr/$coreName/")

  override def put_(data: Seq[Set[Data]]): Unit = {}

  override def put_(key: Key, data: Set[Data]): Unit = {
    val kToString = keyToString(key)
    data.foreach{dta =>
      val document = serializer.serializeToJson(dta)
      val doc = JsObject(document.fields +
        ("_id_" -> JsString(kToString), "id" -> JsString(s"${kToString}___${dta.getId()}_${Random.nextInt(99999)}")))
      addDocument(doc, kToString)
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
      case (dta, map) => map + (serializer.serialize(dta) -> ())
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
      case (dta, map) => map + (serializer.serialize_(dta) -> ())
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

  override def iterator(): Iterator[Set[Data]] = throw new CallNotAllowException("Solr does not easily allow an iterator of sets.", None)

  override def iterator(key: Key): Iterator[Data] = new SolrIterator[Data](serializer, coreName, solrLocation, "_id_:\"" + keyToString(key) + "\"")

  override def get(key: Key): Set[Data] = {
    getDocuments(keyToString(key)).foldRight(Set[Data]()) {
      case (jsObj, set) => set + serializer.deserialize_(jsObj.toString)
    }
  }

  override def all(): Seq[Set[Data]] = {
    getAllDocuments.foldRight(Map[JsString, Set[Data]]()) {
      case (jsObj, data) =>
        jsObj.fields.get("_id_") match{
          case Some(id: JsString) => data.get(id) match {
            case Some(set) => data + (id -> (set + serializer.deserialize_(jsObj.toString)))
            case None => data + (id -> Set(serializer.deserialize_(jsObj.toString)))
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

class SolrIterator[Data <: Identifiable[Data]](serializer: JsonSerializer[Data], coreName: String = "gettingstarted", solrLocation: String = "localhost:8983", query: String = "*:*") extends Iterator[Data] with SolrSpecific[Data] {
  init(coreName, s"http://$solrLocation/solr/$coreName/")
  var currentOffset = 0
  var currentDataList: List[Data] = List()

  def nextDocuments(): List[Data] = {
    val nextDocs = getDocumentsFromQuery(s"select?q=$query&rows=10&start=$currentOffset&wt=json").foldRight(List[Data]()){
      case (doc, dataList) => serializer.deserialize_(new JsObject(doc.fields -- List("id", "_version_", "_id_")).toString()) :: dataList
    }
    currentOffset += 10
    nextDocs
  }

  override def next(): Data = currentDataList match{
    case Nil => nextDocuments() match{
      case Nil => throw new Exception("The SolrIterator does not have another value.")
      case head :: rest =>
        currentDataList = rest
        head
    }
    case head :: rest =>
      currentDataList = rest
      head
  }

  override def hasNext: Boolean = currentDataList match{
    case Nil => nextDocuments() match{
      case Nil => false
      case _ => true
    }
    case _ => true
  }

}