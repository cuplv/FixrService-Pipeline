/**
  * Created by chanceroberts on 5/18/17.
  */
package edu.colorado.plv.fixrservice.pipeline

class ComputeStep[A,B](func: (A => B), datMapType: String, databaseName: String = "test") {
  val statusMap: DataMap[String, A] = datMapType match {
    case "Null" => new NullMap[String, A]
    case "Heap" => new HeapMap[String, A]
  }
  def IncrementalCompute(): Unit = {
    //statusMap
    ???
  }


}
