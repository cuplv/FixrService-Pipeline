/**
  * Created by chanceroberts on 5/23/17.
  */
import edu.colorado.plv.fixr.fixrservice.pipeline._
import org.scalatest._

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

  //WARNING: THESE TESTS ONLY WORK IF YOU HAVE AN INSTANCE OF SOLR RUNNING ON YOUR COMPUTER.
  //DO NOT UNCOMMENT THESE TESTS IF YOU DON'T HAVE AN INSTANCE OF SOLR RUNNING ON YOUR COMPUTER.
  /*
  "SolrDBMap" should "get nothing if nothing is in it" in {
    val sMap = new SolrMap[String, String]("gettingstarted","collection")
    val getVal = sMap.get("Testing")
    assert(getVal.isEmpty)
  }

  it should "be able to put, get, and getAllKeys with the specified fields" in {
    val sMap = new SolrMap[String, String]("gettingstarted", "addition")
    val sMap2 = new SolrMap[String, String]("gettingstarted", "replacement")
    val sMap3 = new SolrMap[String, String]("gettingstarted", "listKeyValues")
    sMap.put("id_1", "Test_1")
    sMap.put("id_2", "Test_2")
    sMap.put("id_3", "Test_3")
    sMap2.put("id_1", "ReplaceThis")
    sMap2.put("id_1", "1001001001")
    sMap3.put("id_1", ":)")
    sMap3.put("id_2", "?")
    sMap3.put("id_3", "(!)")
    sMap3.put("id_4", "...?")
    sMap3.put("id_1", "):)")
    assert(sMap.get("id_1").contains("Test_1"))
    assert(sMap.get("id_2").contains("Test_2"))
    assert(sMap.get("id_3").contains("Test_3"))
    assert(sMap2.get("id_1").contains("1001001001"))
    assert(sMap3.getAllKeys.contains("id_1"))
    assert(sMap3.getAllKeys.contains("id_2"))
    assert(sMap3.getAllKeys.contains("id_3"))
    assert(sMap3.getAllKeys.contains("id_4"))
  }

  it should "be able to put multiple stuff in it" in {
    val sMap = new SolrMap[String, String]("gettingstarted", "addition")
    sMap.put("id_1", "Test_1")
    sMap.put("id_2", "Test_2")
    sMap.put("id_3", "Test_3")
    assert(sMap.get("id_1").contains("Test_1"))
    assert(sMap.get("id_2").contains("Test_2"))
    assert(sMap.get("id_3").contains("Test_3"))
  }

  it should "be able to replace stuff put in it" in {
    val sMap = new SolrMap[String, String]("gettingstarted", "replacement")
    sMap.put("id_1", "10101010")
    sMap.put("id_1", "This is a test")
    sMap.put("id_2", "?")
    assert(sMap.get("id_1").contains("This is a test"))
    assert(sMap.get("id_2").contains("?"))
  }

  it should "be able to return all of the keys in a collection" in {
    val sMap = new SolrMap[String, String]("gettingstarted", "list")
    val sMap2 = new SolrMap[String, String]("gettingstarted", "0")
    sMap.put("id_1", "!")
    sMap.put("id_2", ">")
    sMap.put("id_1", "?")
    sMap.put("id_3", "$")
    sMap.put("id_4", ")=)")
    sMap2.put("id_5", "(!)...:(")
    assert(sMap.getAllKeys.contains("id_1"))
    assert(sMap.getAllKeys.contains("id_2"))
    assert(sMap.getAllKeys.contains("id_3"))
    assert(sMap.getAllKeys.contains("id_4"))
    assert(!sMap.getAllKeys.contains("id_5"))
  }
  */

  //WARNING: THESE TESTS ONLY WORK IF YOU HAVE AN INSTANCE OF THE DATAMAPSERVICE RUNNING.
  //DO NOT UNCOMMENT THESE TESTS IF YOU DON'T HAVE AN INSTANCE OF THE DATAMAPSERVICE RUNNING.
  /*
  "DataMapServiceClient" should "get nothing if nothing is in it" in {
    val dbSC = new DataMapWebServiceClient[String, String]()
    assert(dbSC.get("Not+there").isEmpty)
  }

  "DataMapServiceClient" should "be able to put and add stuff" in {
    val dbSC = new DataMapWebServiceClient[String, String]()
    dbSC.put("id_1", "10101010")
    dbSC.put("id_1", "This is a test")
    dbSC.put("id_2", "?")
    assert(dbSC.get("id_1").contains("This is a test"))
    assert(dbSC.get("id_2").contains("?"))
    dbSC.put("id_1", "")
    dbSC.put("id_2", "")
  }
  */
}
