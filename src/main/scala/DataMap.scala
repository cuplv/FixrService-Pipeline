/**
  * Created by chanceroberts on 5/18/17.
  */
abstract class DataMap[K,V](val tableName: String, val IP: String, val port: Int) {

  def get(k: K) : Option[V]

  def getM(k: K) : V

  def put(k: K, v: V) : Unit
}

class MongoDB[K,V](val tName: String, val ip: String, val prt: Int) extends DataMap[K,V](tName, ip, prt) {
  def get(k: K) : Option[V] = ???

  def getM(k: K) : Option[V] = ???

  def put(k: K, v: V) : Unit = ???
}

class Heap[K,V](val tName: String, val ip: String = "", val prt: Int = 0) extends DataMap[K,V](tName, ip, prt) {
  //Probably needs to be restructured so we don't use var.
  var map: Map[K,V] = Map.empty
  def get(k: K) : Option[V] = map.get(k)

  def getM(k: K) : V = map.get(k) match{
    case None => throw Exception
    case Some(x) => x
  }

  def put(k: K, v: V) : Unit = {
    //Needs to be redone, probably, to keep this as functional programming.
    map = map + (k->v)
    ()
  }
}

class Null[K,V](val tName: String, val ip: String, val prt: Int) extends DataMap[K,V](tName, ip, prt) {
  def get(k: K) : Option[V] = Some(k.asInstanceOf[V])

  def getM(k: K) : V = k.asInstanceOf[V]

  def put(k: K, v: V) : Unit = ()
}