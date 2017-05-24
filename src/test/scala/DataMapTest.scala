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

  it should "return an empty list when listing all its keys" in {
    val nMap = new NullMap[String, String]
    nMap.put("Yes", "No")
    nMap.put("Non-existant","Existant")
    nMap.put("Test", "Test2")
    nMap.put("Test", "Test3")
    val allKeys = nMap.getAllKeys
    assert(allKeys.isEmpty)
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

  it should "be able to overwrite something that was already in the DataMap" in {
    val hMap = new HeapMap[String, String]
    hMap.put("key", "value1")
    hMap.put("key", "value2")
    val getVal = hMap.get("key")
    assert(getVal.contains("value2"))
  }

  it should "be able to list all values put inside of it" in {
    val hMap = new HeapMap[String, String]
    hMap.put("key1", "value1")
    hMap.put("key2", "value2")
    hMap.put("key3", "value3")
    val allKeys = hMap.getAllKeys
    assert(allKeys == List("key1", "key2", "key3"))
  }

  it should "only list a key value once when changed multiple times" in {
    val hMap = new HeapMap[String, String]
    hMap.put("key", "value1")
    hMap.put("Key2", "value1")
    hMap.put("key", "value2")
    hMap.put("Key3", "value2")
    hMap.put("key", "value3")
    val allKeys = hMap.getAllKeys
    assert(allKeys == List("key", "Key2", "Key3"))
  }

  //Need to add MongoDB Tests (Somehow...)
  /*"MongoDBMap" should "get nothing if nothing is in it" in {

    val mDBMap = new MongoDBMap[String, String]("test","collection")
    val getVal = mDBMap.get("Testing")
    assert(getVal.isEmpty)
  }*/
}
