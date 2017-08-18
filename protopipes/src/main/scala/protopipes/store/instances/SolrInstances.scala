package protopipes.store.instances

import protopipes.data.Identifiable
import protopipes.store.{DataMap, DataStore}

import scala.util.parsing.json.JSON
import scalaj.http.Http
import spray.json._


/**
  * Created by chanceroberts on 8/10/17.
  */

case class SolrClassObject(name: String, args: List[Any])

object SolrInstances{
  def createIdDataMap[Data <: Identifiable[Data]](name: String): DataStore[Data] = {
    ???
  }
}

abstract class SolrDataMap[Key, Data <: Identifiable[Data]](coreName: String, solrLocation: String = "localhost:8983") extends DataMap[Key, Data]{
  val url: String = s"http://$solrLocation/solr/$coreName/"

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

  def addToDocument(doc: Map[String, Any], key: Key): Unit = {
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

  override def put_(key: Key, data: Data): Unit = {
    val kToString = keyToString(key)
    val dMap: String = data match{
      case i: Identifiable[_] => toJSON(Map("id" -> i.getId()))
      case _ => toJSON(data)
    }
    val document = getDocument() match{
      case Some(m: Map[String @ unchecked, Any @ unchecked]) =>
        val newM = m - "_version_"
        newM + ("dataID" -> dMap, "data" -> toJson(data))
      case None =>
        Map("id" -> kToString, "dataID" -> dMap, "data" -> toJson(data))
    }
    addToDocument(document, key)
  }

  override def put_(data: Seq[Data]): Unit = { }

  override def get(key: Key): Option[Data] = {
    getDocument() match{
      case Some(m: Map[String @ unchecked, Any @ unchecked]) => m.get("data") match{
        case Some(data: String) => Some(fromJson(data))
        case Some((data: String) :: _) => Some(fromJson(data))
        case _ => None
      }
      case None => None
    }
  }

  override def contains(key: Key): Boolean = getDocument() match{
    case Some(m: Map[String @ unchecked, Any @ unchecked]) => true
    case _ => false
  }

  override def all(): Seq[Data] = {
    val queryURL = url+"select?&rows=100000000&q=*:*"
    val json = Http(queryURL).asString.body
    JSON.parseFull(json) match {
      case Some(parsed: Map[String@unchecked, Any@unchecked]) => parsed.get("response") match {
        case Some(response: Map[String@unchecked, Any@unchecked]) => response.get("docs") match {
          case Some(docs: List[Map[String, Any]@unchecked]) => docs.foldRight(List[Data]()) {
            case (map: Map[String @ unchecked, Any @ unchecked], data) =>
              map.get("data") match{
                case Some(jsonable: String) => fromJson(jsonable) :: data
              }
          }
        }
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

class SolrIterator[Data] extends Iterator[Data] {

  override def next(): Data = ???

  override def hasNext: Boolean = ???

}