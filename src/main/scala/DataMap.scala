/**
  * Created by chanceroberts on 5/18/17.
  */
package edu.colorado.plv.fixr.pipeline

import com.mongodb.casbah.Imports._

abstract class DataMap[K,V](val databaseName: String, val tableName: String, val username: String, val password: String, val IP: String, val port: Int) {
  def get(k: K) : Option[V]


  def getM(k: K) : V = get(k) match{
    case Some(x) => x
    case None => throw new Exception
  }

  def put(k: K, v: V) : Unit
}

class MongoDBMap[K, V](val dName: String, val tName: String, val uName: String = "", val pssWord: String = "", val ip: String = "localhost", val prt: Int = 27017) extends DataMap[K,V](dName, tName, uName, pssWord, ip, prt) {
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
}

//class HeapMap[K,V](val tName: String = "", val uName: String = "", val pssWord: String = "", val ip: String = "", val prt: Int = 0) extends DataMap[K,V](tName, uName, pssWord, ip, prt) {
class HeapMap[K,V] extends DataMap[K,V]("","","","","",0){
  var map: Map[K,V] = Map.empty
  def get(k: K) : Option[V] = map.get(k)

  def put(k: K, v: V) : Unit = {
    map = map + (k->v)
    ()
  }
}

//class NullMap[K,V](val tName: String = "", val uName: String = "", val pssWord: String = "", val ip: String = "", val prt: Int = 0) extends DataMap[K,V](tName, uName, pssWord, ip, prt) {
class NullMap[K,V] extends DataMap[K,V]("","","","","",0){
  def get(k: K) : Option[V] = Some(k.asInstanceOf[V])

  def put(k: K, v: V) : Unit = ()
}