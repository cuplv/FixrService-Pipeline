package protopipes.store.instances

import protopipes.data.Identifiable
import protopipes.store.{DataMap, DataStore}

import scala.util.parsing.json.JSON
import scalaj.http.Http

/**
  * Created by chanceroberts on 8/10/17.
  */

object SolrInstances{
  def createIdDataMap[Data <: Identifiable[Data]](name: String): DataStore[Data] = {
    ???
  }
}

class SolrDataMap[Key, Data] extends DataMap[Key, Data]{
  val url: String = ???

  def keyToString(key: Key) = {
    val (keyID, keyVersion) = key match{
      case i: Identifiable[_] => (i.getId(), i.getVersion())
      case _ => (toJson(key), None)
    }
    keyID + (keyVersion match{
      case Some(s) => "&&v."+s
      case None => ""
    })
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
    case _ => a.toString
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

  def updateDocument(doc: Map[String, Any], keys: List[Key]): Unit = {
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

  override def put_(key: Key, data: Data): Unit = {
    val kToString = keyToString(key)
    val dMap: String = data match{
      case i: Identifiable[_] => toJson(i.getVersion() match{
        case Some(vers) => Map("id" -> i.getId(), "version" -> i.getVersion())
        case None => Map("id" -> i.getId())
      })
      case _ => toJson(data)
    }
    val document = getDocument() match{
      case Some(m: Map[String @ unchecked, String @ unchecked]) =>
        val newM = m - "_version_"
        newM + (kToString -> dMap)
      case None =>
        Map("id" -> name, kToString -> dMap)
    }
    updateDocument(document, List(key))
  }

  override def put_(data: Seq[Data]): Unit = { }

  override def get(key: Key): Option[Data] = ???

  override def all(): Seq[Data] = ???

  override def remove(key: Key): Unit = getDocument() match{
    case Some(doc: Map[String@unchecked, String@unchecked]) =>
      val kToString = keyToString(key)
      val newDoc = doc - "_version_" - kToString
      updateDocument(doc, List(key))
    case None => ()
  }

  override def remove(keys: Seq[Key]): Unit = getDocument() match{
    case Some(doc: Map[String@unchecked, String@unchecked]) =>
      updateDocument(keys.foldLeft(doc - "_version_"){
        case (map, key) =>
          keyToString(key)
          map - keyToString(key)
      }, keys.toList)
    case None => ()
  }

  override def extract(): Seq[Data] = ???

  override def size(): Int = getDocument() match{
    case Some(doc: Map[String@unchecked, String@unchecked]) => doc.foldRight(0){
      case ((key, data), fields) =>
        if (!key.equals("id") && !key.equals("_version_")) fields+1
        else fields
    }
    case None => 0
  }
}