package protopipes.store.instances

import protopipes.data.Identifiable
import protopipes.store.{DataMap, DataMultiMap, DataStore}

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

trait SolrSpecific[Key, Data <: Identifiable[Data]] {
  var name = ""
  var url = ""
  def init(nm: String, URL: String): Unit = {
    name = nm
    url = URL
  }

  def keyToString(key: Key): String = {
    key match {
      case i: Identifiable[_] => i.getId().toString
      case _ => toJSON(key)
    }
  }

  def toJson(d: Data): String

  def fromJson(s: String): Data

  def toJSON(a: Any): String = a match{
    case l: List[_] =>
      l.tail.foldLeft(s"[ ${toJSON(l.head)}") {
        case (str, next) => s"$str, ${toJSON(next)}"
      } + " ]"
    case m: Map[_, _] => m.tail.foldLeft(s" [ ${toJSON(m.head._1)}: ${toJSON(m.head._2)}"){
      case (str, (k, v)) => s"$str, ${toJSON(k)}: ${toJSON(v)}"
    } + " }"
    case s: String => "\"" + s.replaceAll("\"", "\\\"") + "\""
    case _ => a.toString.replaceAll("\"", "\\\"")
  }

  def getDocument(id: String = name): Option[Map[String, Any]] = {
    def getDocumentFromList(list: List[Map[String, Any]], id: String = name): Option[Map[String, Any]] = list match{
      case Nil => None
      case first :: rest => first.get("id") match{
        case Some(x) if x.equals(id) => Some(first)
        case _ => getDocumentFromList(rest, id)
      }
    }
    val queryURL = url+"select?wt=json&q=id=\"" + id + "\""
    val json = Http(queryURL).asString.body
    JSON.parseFull(json) match{
      case Some(parsed: Map[String@unchecked, Any@unchecked]) => parsed.get("response") match{
        case Some(response: Map[String@unchecked, Any@unchecked]) => response.get("docs") match{
          case Some(docs: List[Map[String, Any]@unchecked]) => getDocumentFromList(docs)
        }
        case None => None
      }
      case None => None
    }
  }

  def getDocuments(id: String = name, field: String = "_id_", rows: String = "1000000000"): List[Map[String, Any]] = {
    val queryURL = url+"select?wt=json&rows=" + rows + "&q="+field+"=\""+ id + "\""
    val json = Http(queryURL).asString.body
    JSON.parseFull(json) match{
      case Some(parsed: Map[String@unchecked, Any@unchecked]) => parsed.get("response") match{
        case Some(response: Map[String@unchecked, Any@unchecked]) => response.get("docs") match{
          case Some(docs: List[Map[String, Any]@unchecked]) => docs.foldRight(List[Map[String, Any]]()){
            case (m: Map[String, Any], list) => m.get(field) match{
              case Some(str: String) if str.equals(id) => m :: list
              case _ => list
            }
          }
        }
        case None => List()
      }
      case None => List()
    }
  }

  def addDocument(doc: Map[String, Any], key: Key): Unit = {
    val jsonMap = Map("add" -> Map("doc" -> doc), "commit" -> Map[String, Any]())
    val json = toJSON(jsonMap)
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    JSON.parseFull(result) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("error") match{
          case Some(x) => throw new Exception(s"Could not update the DataMap $name on key(s) $key.")
          case _ => ()
        }
      case _ => ()
    }
  }

  def deleteDocuments(ids: List[String]): Unit = {
    val jsonMap = Map("delete" -> ids)
    val json = toJSON(jsonMap)
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    JSON.parseFull(result) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("error") match{
          case Some(x) => throw new Exception(s"Could not update the DataMap $name on ID(s) $ids.")
          case _ => ()
        }
      case _ => ()
    }
  }

  def getAllDocuments: List[Map[String, Any]] = {
    val queryURL = url+"select?&rows=100000000&q=*:*"
    val json = Http(queryURL).asString.body
    JSON.parseFull(json) match {
      case Some(parsed: Map[String@unchecked, Any@unchecked]) => parsed.get("response") match {
        case Some(response: Map[String@unchecked, Any@unchecked]) => response.get("docs") match {
          case Some(docs: List[Map[String, Any]@unchecked]) => docs
          case _ => List()
        }
        case _ => List()
      }
      case _ => List()
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
    val dMap: String = data match {
      case i: Identifiable[_] => toJSON(Map("id" -> i.getId()))
      case _ => toJSON(data)
    }
    val document = getDocument(kToString) match {
      case Some(m: Map[String@unchecked, Any@unchecked]) =>
        val newM = m - "_version_"
        newM + ("dataID" -> dMap, "data" -> toJson(data))
      case None =>
        Map("id" -> kToString, "dataID" -> dMap, "data" -> toJson(data))
    }
    addDocument(document, key)
  }

  override def put_(data: Seq[Data]): Unit = {}

  override def get(key: Key): Option[Data] = {
    getDocument() match {
      case Some(m: Map[String@unchecked, Any@unchecked]) => m.get("data") match {
        case Some(data: String) => Some(fromJson(data))
        case Some((data: String) :: _) => Some(fromJson(data))
        case _ => None
      }
      case None => None
    }
  }

  override def contains(key: Key): Boolean = getDocument() match {
    case Some(m: Map[String@unchecked, Any@unchecked]) => true
    case _ => false
  }

  override def all(): Seq[Data] = {
    getAllDocuments.foldRight(List[Data]()) {
      case (map: Map[String@unchecked, Any@unchecked], data) =>
        map.get("data") match {
          case Some(jsonable: String) => fromJson(jsonable) :: data
        }
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
    JSON.parseFull(json) match{
      case Some(parsed: Map[String@unchecked, Any@unchecked]) => parsed.get("response") match{
        case Some(response: Map[String@unchecked, Any@unchecked]) => parsed.get("numFound") match {
          case Some(num: Int) => num
          case Some(str: String) => str.toInt
          case _ => 0
        }
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
      val dMap: String = dta match {
        case i: Identifiable[_] => toJSON(Map("id" -> i.getId()))
        case _ => toJson(dta)
      }
      val doc = Map[String, Any]("id" -> s"${kToString}___$dMap", "data" -> toJson(dta), "dataID" -> dMap, "_id_" -> kToString)
      addDocument(doc, key)
    }
  }

  override def remove(keys: Seq[Key]): Unit = {
    keys.foreach(key => remove(key))
  }

  override def remove(key: Key): Unit = {
    deleteDocuments(getDocuments(keyToString(key)).foldRight(List[String]()) {
      case (map, list) => map.get("id") match {
        case Some(s: String) => s :: list
        case _ => list
      }
    })
  }

  override def remove(key: Key, data: Set[Data]): Unit = {
    val serializedMap = data.foldRight(Map[String, Unit]()) {
      case (dta, map) => map + (toJson(dta) -> ())
    }
    deleteDocuments(getDocuments(keyToString(key)).foldRight(List[String]()) {
      case (map, toRemove) => (map.get("id"), map.get("data")) match {
        case (Some(id: String), Some(dt: String)) => serializedMap.get(dt) match {
          case Some(_) => id :: toRemove
          case _ => toRemove
        }
      }
    })
  }

  override def remove(data: Set[Data]): Unit = {
    val serializedMap = data.foldRight(Map[String, Unit]()) {
      case (dta, map) => map + (toJson(dta) -> ())
    }
    val toDelete = getAllDocuments.foldRight(List[String]()){
      case (doc, toRemove) => (doc.get("id"), doc.get("data")) match{
        case (Some(id: String), Some(dta: String)) => serializedMap.get(dta) match{
          case Some(_) => id :: toRemove
          case _ => toRemove
        }
      }
    }
    deleteDocuments(toDelete)
  }

  override def iterator(): Iterator[Set[Data]] = ???

  override def iterator(key: Key): Iterator[Data] = ???

  override def get(key: Key): Set[Data] = {
    getDocuments(keyToString(key)).foldRight(Set[Data]()) {
      case (m, set) => m.get("data") match {
        case Some(str: String) => set + fromJson(str)
        case None => set
      }
    }
  }

  override def all(): Seq[Set[Data]] = {
    getAllDocuments.foldRight(Map[String, Set[Data]]()) {
      case (map: Map[String@unchecked, Any@unchecked], data) =>
        (map.get("data"), map.get("_id_")) match {
          case (Some(jsonable: String), Some(iden: String)) => data.get(iden) match {
            case Some(x) => data + (iden -> (x + fromJson(jsonable)))
            case None => data + (iden -> Set(fromJson(jsonable)))
          }
          case _ => data
        }
    }.toList.unzip._2
  }

  override def contains(key: Key, data: Set[Data]): Boolean = data subsetOf get(key)

  override def extract(): Seq[Set[Data]] = {
    val (mapOfSets, stuffToRemove) = getAllDocuments.foldRight((Map[String, Set[Data]](), List[String]())) {
      case (map: Map[String@unchecked, Any@unchecked], (data, toRemove)) =>
        (map.get("data"), map.get("_id_"), map.get("id")) match {
          case (Some(jsonable: String), Some(iden: String), Some(id: String)) => data.get(iden) match {
            case Some(x) => (data + (iden -> (x + fromJson(jsonable))), id :: toRemove)
            case None => (data + (iden -> Set(fromJson(jsonable))), id :: toRemove)
          }
          case _ => (data, toRemove)
        }
    }
    deleteDocuments(stuffToRemove)
    mapOfSets.toSeq.unzip._2
  }

  override def size(): Int = {
    getAllDocuments.foldRight(Map[String, Unit]()) {
      case (map, keysFound) =>
        map.get("_id_") match {
          case Some(x: String) => keysFound.get(x) match {
            case Some(_) => keysFound
            case None => keysFound + (x -> ())
          }
        }
    }.size
  }
}

class SolrIterator[Data] extends Iterator[Data] {

  override def next(): Data = ???

  override def hasNext: Boolean = ???

}