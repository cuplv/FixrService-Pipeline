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

  //WARNING: THESE TESTS ONLY WORK IF YOU HAVE AN INSTANCE OF MONGODB RUNNING ON YOUR COMPUTER.
  //DO NOT UNCOMMENT THESE TESTS IF YOU DON'T HAVE AN INSTANCE OF MONGODB RUNNING ON YOUR COMPUTER.
  /*
  "MongoDBMap" should "get nothing if nothing is in it" in {
    val mDBMap = new MongoDBMap[String, String]("test","collection")
    val getVal = mDBMap.get("Testing")
    assert(getVal.isEmpty)
  }

  it should "be able to put multiple stuff in it" in {
    val mDBMap = new MongoDBMap[String, String]("test", "addition")
    mDBMap.put("id_1", "Test_1")
    mDBMap.put("id_2", "Test_2")
    mDBMap.put("id_3", "Test_3")
    assert(mDBMap.get("id_1").contains("Test_1"))
    assert(mDBMap.get("id_2").contains("Test_2"))
    assert(mDBMap.get("id_3").contains("Test_3"))
  }

  it should "be able to replace stuff put in it" in {
    val mDBMap = new MongoDBMap[String, String]("test", "replacement")
    mDBMap.put("id_1", "10101010")
    mDBMap.put("id_1", "This is a test")
    mDBMap.put("id_2", "?")
    assert(mDBMap.get("id_1").contains("This is a test"))
    assert(mDBMap.get("id_2").contains("?"))
  }

  it should "be able to return all of the keys in a collection" in {
    val mDBMap = new MongoDBMap[String, String]("test", "list")
    mDBMap.put("id_1", "!")
    mDBMap.put("id_2", ">")
    mDBMap.put("id_1", "?")
    mDBMap.put("id_3", "$")
    mDBMap.put("id_4", ")=)")
    assert(mDBMap.getAllKeys.equals(List("id_1", "id_2", "id_3", "id_4")))
  }
  */
}
