package protopipes.store.instances

import protopipes.data.Identifiable
import protopipes.data.serializers.JsonSerializer
import protopipes.exceptions.{CallNotAllowException, NotInitializedException, ProtoPipeException}
import protopipes.store.{DataMap, DataMultiMap, DataStore}
import spray.json._

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

class SolrBackend[Data <: Identifiable[Data]](nam: String, url: String){
  var templateOpt: Option[JsObject] = None

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
        case Some(docs: JsArray) => docs.elements.toList.map{doc => mkDocDeserializable(doc.asJsObject)}
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
    val docsToCheck = getDocumentsFromQuery("select?wt=json&q=id:\"" + id + "\"")
    getDocumentFromList(docsToCheck, id)
  }

  def getDocuments(id: String, field: String = "_id_"): List[JsObject] = {
    val docsToCheck = getDocumentsFromQuery("select?wt=json&rows=1000000000&q="+field+":\""+ id + "\"")
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
    templateOpt match{
      case None => templateOpt = Some(doc)
      case _ => ()
    }
  }

  def deleteDocuments(ids: List[String]): Unit = {
    val json = JsObject(Map("delete" -> JsArray(ids.map(JsString(_)).toVector), "commit" -> JsObject())).toString
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    result.parseJson.asJsObject.fields.get("error") match{
      case Some(x) => throw new Exception(s"Could not update the core $nam on ID(s) $ids.")
      case _ => ()
    }
  }

  def mkDocDeserializable(doc: JsObject): JsObject = templateOpt match{
    case None => doc
    case Some(template) =>
      JsObject(template.fields.map{
        case (key, jsValue) => (jsValue, doc.fields.get(key)) match{
          case (thisValue, None) => thisValue.getClass.getName match{
            case "spray.json.JsArray" => (key, JsArray())
            case "spray.json.JsString" => (key, JsString(""))
            //Next line should NEVER happen as of this version, but I may add functionality for that later?
            case "spray.json.JsObject" => (key, JsObject())
            case s => throw new Exception(s"Cannot have a null version of $s...")
          }
          case (_: JsArray, Some(trueValue: JsArray)) => (key, trueValue)
          case (_: JsArray, Some(trueValue)) => (key, JsArray(trueValue))
          case (_, Some(trueValue: JsArray)) => (key, trueValue.elements.head)
          case (_, Some(trueValue)) => (key, trueValue)
        }
      })
  }

  def getAllDocuments: List[JsObject] = getDocumentsFromQuery("select?wt=json&rows=100000000&q=*:*")
}

class SolrDataMap[Key, Data <: Identifiable[Data]](serializer: JsonSerializer[Data], coreName: String, solrLocation: String = "localhost:8983", needToCreate: Boolean = false) extends DataMap[Key, Data] {
  if (needToCreate) {
    Http(s"http://$solrLocation/solr/admin/collections?action=CREATE&name=$coreName&numShards=1").asString.body
  }
  val url = s"http://$solrLocation/solr/$coreName/"
  val solrBack: SolrBackend[Data] = new SolrBackend[Data](coreName, url)

  override def put_(key: Key, data: Data): Unit = {
    val kToString = solrBack.keyToString(key)
    val document = serializer.serializeToJson(data)
    val doc = JsObject(document.fields + ("id" -> JsString(kToString)))
    solrBack.addDocument(doc, kToString)
  }

  override def put_(data: Seq[Data]): Unit = {}

  override def get(key: Key): Option[Data] = {
    solrBack.getDocument(solrBack.keyToString(key)) match{
      case Some(jsObject: JsObject) =>
        Some(serializer.deserialize_(JsObject(jsObject.fields -- List("id")).toString))
      case _ => None
    }
  }

  override def contains(key: Key): Boolean = solrBack.getDocument(solrBack.keyToString(key)) match {
    case Some(_) => true
    case _ => false
  }

  override def all(): Seq[Data] = {
    solrBack.getAllDocuments.foldRight(List[Data]()) {
      case (jsObj, data) =>
        val jsonable = JsObject(jsObj.fields -- List("id")).toString
        serializer.deserialize_(jsonable) :: data
    }
  }

  override def remove(key: Key): Unit = {
    solrBack.deleteDocuments(List(solrBack.keyToString(key)))
  }

  override def remove(keys: Seq[Key]): Unit = {
    solrBack.deleteDocuments(keys.foldRight(List.empty[String]){
      case (key, list) => solrBack.keyToString(key) :: list
    })
  }

  override def extract(): Seq[Data] = {
    val toReturn = all()
    Http(url+"update").postData("{\"delete\": { \"query\":\"*:*\"}, \"commit\": {}}").header("Content-Type", "application/json").asString.body
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

  override def iterator(): Iterator[Data] = new SolrIterator[Data](serializer, solrBack, coreName, solrLocation)
}

class SolrMultiMap[Key, Data <: Identifiable[Data]](serializer: JsonSerializer[Data], coreName: String, solrLocation: String = "localhost:8983", needToCreate: Boolean = false) extends DataMultiMap[Key, Data] {
  if (needToCreate) {
    Http(s"http://$solrLocation/solr/admin/collections?action=CREATE&name=$coreName&numShards=1").asString.body
  }
  val url = s"http://$solrLocation/solr/$coreName/"
  val solrBack: SolrBackend[Data] = new SolrBackend[Data](coreName, s"http://$solrLocation/solr/$coreName/")

  override def put_(data: Seq[Set[Data]]): Unit = {}

  override def put_(key: Key, data: Set[Data]): Unit = {
    val kToString = solrBack.keyToString(key)
    data.foreach{dta =>
      val document = serializer.serializeToJson(dta)
      val doc = JsObject(document.fields +
        ("_id_" -> JsString(kToString), "id" -> JsString(s"${kToString}___${dta.getId()}_${dta.toString}")))
      solrBack.addDocument(doc, kToString)
    }
  }

  override def remove(keys: Seq[Key]): Unit = {
    keys.foreach(key => remove(key))
  }

  override def remove(key: Key): Unit = {
    solrBack.deleteDocuments(solrBack.getDocuments(solrBack.keyToString(key)).foldRight(List[String]()) {
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
    solrBack.deleteDocuments(solrBack.getDocuments(solrBack.keyToString(key)).foldRight(List[String]()){
      case (jsObj, toRemove) =>
        serializedMap.get(JsObject(jsObj.fields -- List("id", "_id_")).toString()) match {
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
    solrBack.deleteDocuments(solrBack.getAllDocuments.foldRight(List[String]()){
      case (jsObj, toRemove) =>
        serializedMap.get(JsObject(jsObj.fields -- List("id", "_id_")).toString()) match {
          case Some(_) => jsObj.fields.get("id") match{
            case Some(id: JsString) => id.value :: toRemove
            case _ => toRemove
          }
          case _ => toRemove
        }
    })
  }

  override def iterator(): Iterator[Set[Data]] =
    throw new CallNotAllowException("Solr does not easily allow an iterator of sets.", None)

  override def iterator(key: Key): Iterator[Data] =
    new SolrIterator[Data](serializer, solrBack, coreName, solrLocation, "_id_:\"" + solrBack.keyToString(key) + "\"")

  override def get(key: Key): Set[Data] = {
    solrBack.getDocuments(solrBack.keyToString(key)).foldRight(Set[Data]()) {
      case (jsObj, set) => set + serializer.deserialize_(jsObj.toString)
    }
  }

  override def all(): Seq[Set[Data]] = {
    solrBack.getAllDocuments.foldRight(Map[JsString, Set[Data]]()) {
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
    Http(url+"update").postData("{\"delete\": { \"query\":\"*:*\"}, \"commit\": {}}").header("Content-Type", "application/json").asString.body
    mapOfSets
  }

  override def size(): Int = {
    solrBack.getAllDocuments.foldRight(Map[JsString, Unit]()) {
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

class SolrIterator[Data <: Identifiable[Data]](serializer: JsonSerializer[Data], solrBackend: SolrBackend[Data], coreName: String = "gettingstarted", solrLocation: String = "localhost:8983", query: String = "*:*") extends Iterator[Data] {
  var currentOffset = 0
  var currentDataList: List[Data] = List()

  def nextDocuments(): List[Data] = {
    val nextDocs = solrBackend.getDocumentsFromQuery(s"select?q=$query&rows=10&start=$currentOffset&wt=json").foldRight(List[Data]()){
      case (doc, dataList) => serializer.deserialize_(new JsObject(doc.fields -- List("id", "_id_")).toString()) :: dataList
    }
    currentOffset += 10
    nextDocs
  }

  override def next(): Data = currentDataList match{
    case Nil =>
      nextDocuments() match{
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
      case docs =>
        currentDataList = docs
        true
    }
    case _ => true
  }

}