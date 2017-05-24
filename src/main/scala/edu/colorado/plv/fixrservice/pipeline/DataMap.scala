package edu.colorado.plv.fixrservice.pipeline

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}

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
