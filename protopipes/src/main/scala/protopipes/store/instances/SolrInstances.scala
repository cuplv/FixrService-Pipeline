package protopipes.store.instances

import protopipes.data.Identifiable
import protopipes.store.{DataMap, DataStore}

/**
  * Created by chanceroberts on 8/10/17.
  */

object SolrInstances{
  def createIdDataMap[Data <: Identifiable[Data]](name: String): DataStore[Data] = {
    ???
  }
}

class SolrDataMap[Key, Data] extends DataMap[Key, Data]{
  def toJson(a: Any): String = a match{
    case l: List[_] =>
      l.tail.foldLeft(s"[ ${toJson(l.head)}") {
        case (str, next) => s"$str, ${toJson(next)}"
      } + " ]"
    case m: Map[_, _] => m.tail.foldLeft(s" [ ${toJson(m.head._1)}: ${toJson(m.head._2)}"){
      case (str, (k, v)) => s", ${toJson(k)}: ${toJson(v)}"
    } + " }"
    case s: String => "\"" + s + "\""
    case _ => a.toString
  }

  override def put_(data: Seq[Data]): Unit = {
    ???
  }

  override def put_(key: Key, data: Data): Unit = {
    val (keyID, keyVersion) = key match{
      case i: Identifiable[_] => (i.getId(), i.getVersion())
      case _ => (toJson(key), None)
    }
    val (dataID, dataVersion) = data match{
      case i: Identifiable[_] => (i.getId(), i.getVersion())
      case _ => (toJson(data), None)
    }
    val startingJson =
      """{
        | "add": {
        |   "doc": {
      """.stripMargin
    val json = get(key) match{
      case Some(d: Data) => ???
      case None =>
        startingJson+
          """
            |     "id": ???
          """.stripMargin
        ???
    }
    ???
  }

  override def get(key: Key): Option[Data] = ???

  override def all(): Seq[Data] = ???

  override def remove(key: Key): Unit = ???

  override def remove(keys: Seq[Key]): Unit = ???

  override def extract(): Seq[Data] = ???

  override def size(): Int = ???
}