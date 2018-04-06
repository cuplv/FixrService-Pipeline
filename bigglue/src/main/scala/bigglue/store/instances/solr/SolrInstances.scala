package bigglue.store.instances.solr

import com.typesafe.config.{Config, ConfigFactory}
import bigglue.data.Identifiable
import bigglue.data.serializers.JsonSerializer
import bigglue.exceptions.CallNotAllowException
import bigglue.store.{DataMap, DataMultiMap, DataStore}
import spray.json._

import scalaj.http.Http


/**
  * Created by chanceroberts on 8/10/17.
  */

case class SolrClassObject(name: String, args: List[Any])

object SolrInstances{
  def createDataMap[Data <: Identifiable[Data]](name: String): DataStore[Data] = {
    ???
  }
}

/**
  * This is the default data store that is used in the example.
  * Without going too far into the Backend, this is an abstraction for a Solr Collection.
  * @param serializer The serializer that needs to be used to serialize the objects into a flat Json file.
  * @param coreName The name of the core/collection that is being used. If the core/collection doesn't exist, we just create a new core/collection.
  * @param config The configuration file used for the Solr instance. If not specified, this will just be the default file.
  * @tparam Key  The type of the key that's being used. Within the examples, in the cases of a, b, and c, this is [[bigglue.data.I]][Int].
  *              In the cases of d, this is [[bigglue.examples,Counter]].
  * @tparam Data The type of the data that's being used. Within the examples, in the cases of a, b, and c, this is [[bigglue.data.I]][Int].
  *              In the cases of d, this is [[bigglue.examples.Counter]].
  */
class SolrDataMap[Key, Data <: Identifiable[Data]](serializer: JsonSerializer[Data], coreName: String, config: Config = ConfigFactory.load()) extends DataMap[Key, Data] {
  val solrBack: SolrBackend[Data] = new SolrBackend[Data](coreName, config)
  val url: String = solrBack.url
  //override val serializerOpt = Some(serializer)
  name = coreName

  override def put_(key: Key, data: Data): Unit = {
    val kToString = solrBack.keyToString(key)
    val document = serializer.serializeToJson(data)
    val doc = JsObject(document.fields + ("id" -> JsString(kToString)))
    solrBack.addDocument(doc, kToString)
  }

  override def put_(data: Seq[Data]): Unit = {
    data.foreach{
      dta =>
        val key = dta.getId()
        val document = serializer.serializeToJson(dta)
        val doc = JsObject(document.fields + ("id" -> JsString(key)))
        solrBack.addDocument(doc, key)
    }
  }

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

  override def iterator(): Iterator[Data] = new SolrIterator[Data](serializer, solrBack, coreName)
}

class SolrMultiMap[Key, Data <: Identifiable[Data]](serializer: JsonSerializer[Data], coreName: String, config: Config = ConfigFactory.load()) extends DataMultiMap[Key, Data] {

  val solrBack: SolrBackend[Data] = new SolrBackend[Data](coreName, config)
  val url: String = solrBack.url

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
    new SolrIterator[Data](serializer, solrBack, coreName, "_id_:\"" + solrBack.keyToString(key) + "\"")

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
            case Some(_) => keysFound
            case None => keysFound + (id -> ())
          }
          case _ => keysFound
        }
    }.size
  }
}

class SolrIterator[Data <: Identifiable[Data]](serializer: JsonSerializer[Data], solrBackend: SolrBackend[Data], coreName: String = "gettingstarted", query: String = "*:*") extends Iterator[Data] {
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