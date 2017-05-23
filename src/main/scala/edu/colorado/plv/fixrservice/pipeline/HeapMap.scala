package edu.colorado.plv.fixrservice.pipeline

/**
  * Created by chanceroberts on 5/23/17.
  */
//class HeapMap[K,V](val tName: String = "", val uName: String = "", val pssWord: String = "", val ip: String = "", val prt: Int = 0) extends DataMap[K,V](tName, uName, pssWord, ip, prt) {
class HeapMap[K,V] extends DataMap[K,V]("","","","","",0){
  var map: Map[K,V] = Map.empty
  def get(k: K) : Option[V] = map.get(k)

  def put(k: K, v: V) : Unit = {
    map = map + (k->v)
    ()
  }
}

