package edu.colorado.plv.fixr.fixrservice.pipeline

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import scalaj.http._
import scala.util.parsing.json.JSON


/**
  * Created by chanceroberts on 5/23/17.
  */
abstract class DataMap[K,V](val databaseName: String, val tableName: String, val IP: String, val port: String, val username: String, val password: String) {
  def get(k: K) : Option[V]


  def getM(k: K) : V = get(k) match{
    case Some(x) => x
    case None => throw new Exception
  }

  def put(k: K, v: V) : Unit

  def getAllKeys : List[K]
}

class SolrMap[K, V](val cName: String, val fName: String = "value", val ip: String = "localhost", val prt: String = "8983") extends DataMap[K,V](cName, fName, ip, prt, "", "") {
  val startingURL: String = "http://"+ip+":"+port+"/solr/"+cName+"/"
  def get(k: K): Option[V]  = {
    val queryURL = startingURL+"select?q=key:"+k+"&wt=json"
    val json = Http(queryURL).asString.body //Query the Database Using the URL
    //println(json)
    JSON.parseFull(json) match{
      case Some(parsed: Map[String, Any]) =>
        parsed.get("response") match{
          case Some(resp: Map[String, Any]) =>
            resp.get("docs") match{
              case Some(resp2:  List[Map[String, Any]]) => resp2 match{
                case first :: list =>
                  first.get(fName) match{
                    case Some(List(x)) => Some(x.asInstanceOf[V])
                    case Some(x :: more) => Some(x.asInstanceOf[V])
                    case Some(x) => Some(x.asInstanceOf[V])
                    case None => None
                  }
                case _ => None
              }

              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
  }

  def getObject(k: K): List[(String, Any)] = {
    val queryURL = startingURL+"select?wt=json&q=key:"+k
    val json = Http(queryURL).asString.body //Query the Database Using the URL
    JSON.parseFull(json) match{
      case Some(parsed: Map[String, Any]) =>
        parsed.get("response") match{
          case Some(resp: Map[String, Any]) =>
            resp.get("docs") match{
              case Some((first: Map[String, Any]) :: list) =>
                first.foldRight(List.empty[(String, Any)]){
                  case ((key, value), l) =>
                    (key, value) :: l
                }
              case _ => List.empty[(String, V)]
            }
          case _ => List.empty[(String, V)]
        }
      case _ => List.empty[(String, V)]
    }
  }

  def put(k: K, v: V): Unit = {
    val queryURL = startingURL+"update"
    val jsonValue: String = getObject(k) match{
      case l if l.isEmpty =>
        """{
          |  "add": {
          |    "doc": {
          |      "key": """.stripMargin + (k match {
          case s: String => "\"" + k + "\""
          case _ => k.toString
        }) +
        """,
          |      """.stripMargin + "\"" + fName + "\": " + (v match{
          case s: String => "\"" + v + "\""
          case _ => v.toString
        }) +
        """
          |    }
          |  },
          |  "commit": {}
          |}
        """.stripMargin
      case l =>
        val (mostOfString, needToAddValue) = l.foldLeft(
        """{
          |  "add": {
          |    "doc": {""".stripMargin, true) {
          case ((json, curr), (key, value)) => if (key.equals(fName)) {
            (json + """
              |      """.stripMargin + "\"" + fName + "\": " + (v match {
              case s: String => "\"" + v + "\""
              case vL: List[_] => val tBC = "[ " +  vL.foldLeft("") {
                case (j: String, s: String) => j + "\"" + s + "\", "
                case (j: String, va) => j + va.toString + ", "
              }
                tBC.substring(0,tBC.length-2) + " ]"
              case _ => v.toString
            }) + ",", false)
          } else if (!key.equals("_version_")) {
            (json + """
              |      """.stripMargin + "\"" + key + "\": " + (value match {
              case s: String => "\"" + value + "\""
              case vL: List[_] => val tBC = "[ " +  vL.foldLeft(""){
                case (j: String, s: String) => j + "\"" + s + "\", "
                case (j: String, va) => j + va.toString + ", "
              }
                tBC.substring(0,tBC.length-2) + " ]"
              case _ => value.toString
            }) + ",", curr)
          } else{
            (json,curr)
          }
        }
        val mostOfString2 = if (needToAddValue){
           mostOfString + """
             |      """.stripMargin + "\"" + fName + "\": " + (v match{
             case s: String => "\"" + v + "\""
             case vL: List[_] => val tBC = "[ " +  vL.foldLeft("") {
               case (j: String, s: String) => j + "\"" + s + "\", "
               case (j: String, va) => j + va.toString + ", "
             }
               tBC.substring(0,tBC.length-2) + " ]"
             case _ => v.toString
           }) + ","
        } else {
          mostOfString + ""
        }
        mostOfString2.substring(0, mostOfString2.length-1) +
        """
          |    }
          |  },
          |  "commit": {}
          |}
        """.stripMargin
    }
    //Find a way to POST Request this into Solr
    Http(queryURL).postData(jsonValue.getBytes).header("Content-Type", "application/json").asString.body
  }

  def getAllKeys: List[K] = {
    val queryURL = startingURL+"select?wt=json&rows=1000000&q=*:*"
    val json: String = Http(queryURL).asString.body //Find a way to Query the Database using a URL
    JSON.parseFull(json) match{
      case Some(parsed: Map[String, Any]) =>
        parsed.get("response") match {
          case Some(resp: Map[String, Any]) =>
            resp.get("docs") match {
              case Some(resp2: List[Map[String, Any]]) =>
                resp2.foldRight(List.empty[K]){
                  case (map, l) => map.get("key") match{
                    case Some(List(v)) => map.get(fName) match{
                      case Some(List(v2)) => v.asInstanceOf[K] :: l
                      case _ => l
                    }
                    case _ => l
                  }
                }
              case _ => List.empty[K]
            }
          case _ => List.empty[K]
        }
      case _ => List.empty[K]
    }
  }
}

class MongoDBMap[K, V](val dName: String, val tName: String, val ip: String = "localhost", val prt: String = "27017", val uName: String = "", val pssWord: String = "") extends DataMap[K,V](dName, tName, ip, prt, uName, pssWord) {
  val client = MongoClient(MongoClientURI{
    val startingPoint = "mongodb://"
    val addUserPass = (username, password) match{
      case ("", "") => startingPoint
      case _ => startingPoint + username + ":" + password + "@"
    }
    addUserPass + ip + ":" + port.toString + "/"
  })
  private val database = client(databaseName)
  private val coll = database(tableName)

  def get(k: K) : Option[V] = {
    coll.findOne(MongoDBObject("key" -> k)) match{
      case Some(x) => try {
        Some(x.get("value").asInstanceOf[V])
      } catch {
        case _ : Throwable => None
      }
      case None => None
    }

  }


  def put(k: K, v: V) : Unit = {
    coll.update(MongoDBObject("key" -> k), MongoDBObject("key" -> k, "value" -> v), upsert = true)
    ()
  }

  def getAllKeys: List[K] = {
    val documents = coll.find()
    documents.toList.foldRight(List.empty[K]) {
      case (dbObj, l) =>
        dbObj.get("key").asInstanceOf[K] :: l //If you're playing nice and doing put like you should, the asInstanceOf[K] should do nothing.
    }
  }
}

//class HeapMap[K,V](val tName: String = "", val uName: String = "", val pssWord: String = "", val ip: String = "", val prt: Int = 0) extends DataMap[K,V](tName, uName, pssWord, ip, prt) {
class HeapMap[K,V] extends DataMap[K,V]("","","","","","") {
  var map: Map[K, V] = Map.empty

  def get(k: K): Option[V] = map.get(k)

  def put(k: K, v: V): Unit = {
    map = map + (k -> v)
    ()
  }

  def getAllKeys: List[K] = {
    map.foldRight(List.empty[K]){case ((k,v), l) => k :: l}
  }
}

//class NullMap[K,V](val tName: String = "", val uName: String = "", val pssWord: String = "", val ip: String = "", val prt: Int = 0) extends DataMap[K,V](tName, uName, pssWord, ip, prt) {
class NullMap[K,V] extends DataMap[K,V]("","","","","",""){
  def get(k: K) : Option[V] = Some(k.asInstanceOf[V])

  def put(k: K, v: V) : Unit = ()

  def getAllKeys: List[K] = List.empty[K]
}

class WebServiceClient[K,V](ip: String = "localhost", port: String = "8080") extends DataMap[K,V]("","",ip,port,"",""){
  val webServerAt: String = "http://"+ip+":"+port+"/"
  def get(k: K): Option[V] = {
    val gotten: String = Http(webServerAt+"get/key="+k.toString).asString.body
    JSON.parseFull(gotten) match {
      case m: Map[String, _] => m.get("succ") match{
        case Some(true) => m.get("value").asInstanceOf[Option[V]]
        case _ => None
      }
    }
  }

  def getAllKeys: List[K] = {
    val gotten: String = Http(webServerAt+"getKeys").asString.body
    JSON.parseFull(gotten) match {
      case m: Map[String, Any] => m.get("succ") match{
        case Some(true) => m.get("keys") match{
          case Some(l: List[_]) => l.foldRight(List.empty[K]){
            case (x, list) => x.asInstanceOf[K] :: list
          }
          case _ => List()
        }
        case None => List()
      }
    }
  }

  def put(k: K, v: V): Unit = {
    val json: String = "{ \"key\": " + (k match{
      case s: String => "\"" + s + "\""
      case x => x
    }) + ", \"value\": " + (v match{
      case l: List[_] =>
        val mostOfThis = "[ " + l.foldLeft(""){
          case (str, value) => (value match{
            case s: String => "\"" + s + "\""
            case v: Any => v.toString
          }) + ", "
        }
        mostOfThis.substring(0,mostOfThis.length-2) + " ]"
      case s: String => "\"" + s + "\""
      case x => x
    }) + " }"
    Http(webServerAt+"/put").postData(json)
  }
}