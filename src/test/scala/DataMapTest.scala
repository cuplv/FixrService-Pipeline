/**
  * Created by chanceroberts on 5/23/17.
  */
import org.scalatest._
import edu.colorado.plv.fixrservice.pipeline._

class DataMapTest extends FlatSpec {
  "NullMap" should "return its own input when getting stuff" in {
    val nMap = new NullMap[String, String]
    val getVal = nMap.get("Testing")
    assert(getVal.contains("Testing"))
  }

  it should "not do anything on put" in {
    val nMap = new NullMap[String, String]
    nMap.put("Status", "Testing")
    val getVal = nMap.get("Testing")
    assert(getVal.contains("Testing"))
  }

  "HeapMap" should "get nothing if nothing is in it" in {
    val hMap = new HeapMap[String, String]
    val getVal = hMap.get("Testing")
    assert(getVal.isEmpty)
  }

  it should "be able to put something in the DataMap and get it out" in {
    val hMap = new HeapMap[String, String]
    hMap.put("key", "value")
    val getVal = hMap.get("key")
    assert(getVal.contains("value"))
  }

  //Need to add MongoDB Tests (Somehow...)
  /*"MongoDBMap" should "get nothing if nothing is in it" in {
    val mDBMap = new MongoDBMap[String, String]("dataBase","collection")
    val getVal = mDBMap.get("Testing")
    assert(getVal.isEmpty)
  }*/
}
