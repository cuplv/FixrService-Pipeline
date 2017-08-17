package protopipes.store.instances

import protopipes.data.Identifiable
import protopipes.store.{DataMap, DataStore}

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

class SolrDataMap[Key, Data](coreName: String, solrLocation: String = "localhost:8983") extends DataMap[Key, Data]{
  val url: String = s"http://$solrLocation/solr/$coreName/"

  def extractClassFromData(str: String): SolrClassObject = {

    ???
  }

  def keyToString(key: Key): String = {
    key match {
      case i: Identifiable[_] => i.getId().toString
      case _ => toJson(key)
    }
  }

  def toJson(a: Any): String = a match{
    case l: List[_] =>
      l.tail.foldLeft(s"[ ${toJson(l.head)}") {
        case (str, next) => s"$str, ${toJson(next)}"
      } + " ]"
    case m: Map[_, _] => m.tail.foldLeft(s" [ ${toJson(m.head._1)}: ${toJson(m.head._2)}"){
      case (str, (k, v)) => s"$str, ${toJson(k)}: ${toJson(v)}"
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

  def addToDocument(doc: Map[String, Any], keys: List[Key]): Unit = {
    val jsonMap = Map("add" -> Map("doc" -> doc), "commit" -> Map[String, Any]())
    val json = toJson(jsonMap)
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    JSON.parseFull(result) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("error") match{
          case Some(x) => throw new Exception(s"Could not update the DataMap $name on key(s) $keys.")
          case _ => ()
        }
      case _ => ()
    }
  }

  def deleteDocuments(ids: List[String]): Unit = {
    val jsonMap = Map("delete" -> ids)
    val json = toJson(jsonMap)
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
      case i: Identifiable[_] => toJson(Map("id" -> i.getId()))
      case _ => toJson(data)
    }
    val document = getDocument() match{
      case Some(m: Map[String @ unchecked, Any @ unchecked]) =>
        val newM = m - "_version_"
        newM + ("dataID" -> dMap, "data" -> data.toString)
      case None =>
        Map("id" -> kToString, "dataID" -> dMap, "data" -> data.toString)
    }
    addToDocument(document, List(key))
  }

  override def put_(data: Seq[Data]): Unit = { }

  override def get(key: Key): Option[Data] = getDocument() match{
    case Some(m: Map[String @ unchecked, Any @ unchecked]) => m.get("data") match{
      case Some(data: String) => ???
      case Some((data: String) :: _) => ???
      case _ => None
    }
      ???
    case None => None
  }

  override def contains(key: Key): Boolean = ???

  override def all(): Seq[Data] = ???

  override def remove(key: Key): Unit = {
    deleteDocuments(List(keyToString(key)))
  }

  override def remove(keys: Seq[Key]): Unit = {
    deleteDocuments(keys.foldRight(List.empty[String]){
      case (key, list) => keyToString(key) :: list
    })
  }

  override def extract(): Seq[Data] = ???

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