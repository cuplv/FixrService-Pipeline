package edu.colorado.plv.fixrservice.pipeline

/**
  * Created by chanceroberts on 5/23/17.
  */
//class NullMap[K,V](val tName: String = "", val uName: String = "", val pssWord: String = "", val ip: String = "", val prt: Int = 0) extends DataMap[K,V](tName, uName, pssWord, ip, prt) {
class NullMap[K,V] extends DataMap[K,V]("","","","","",""){
  def get(k: K) : Option[V] = Some(k.asInstanceOf[V])

  def put(k: K, v: V) : Unit = ()
}
