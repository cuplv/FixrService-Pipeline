package edu.colorado.plv.fixrservice.pipeline

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
}
