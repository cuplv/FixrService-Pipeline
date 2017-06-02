/**
  * Created by chanceroberts on 5/23/17.
  */
import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.model.FormData
import akka.http.scaladsl.server
import org.scalatest._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import edu.colorado.plv.fixr.fixrservice.pipeline.{DataMapActor, DataMapService, HeapMap, DataMap}

//These tests need to be rewritten.
class DataMapServiceTest extends FlatSpec with Matchers with ScalatestRouteTest {
  "DataMapService" should "work with post /put and get /get with basic query" in {
    val dMapService = DataMapService
    val testMapMap: HeapMap[String, DataMap[String, Any]] = new HeapMap[String, DataMap[String, Any]]
    val testmap: HeapMap[String, Any] = new HeapMap[String, Any]
    testMapMap.put("test", testmap)
    val testActorMap: HeapMap[String, ActorRef] = new HeapMap[String, ActorRef]
    testActorMap.put("test", system.actorOf(Props(new DataMapActor(testmap)), "testDMapActor"))
    val testRoute: server.Route = dMapService.getCommand(testActorMap)
    Post("/put", "{ \"key\": \"test\", \"value\": \"Hello, world!\", \"dataMap\": \"test\" }") ~> testRoute
    Get("/get?key=test&dataMap=test") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"dataMap\": \"test\", \"key\": \"test\", \"value\": \"Hello, world!\" }"
    }
    testActorMap.getAllKeys.foldLeft(){
      (_, key) => testActorMap.get(key) match{
        case Some(aRef) => system.stop(aRef)
        case None => () //Should never occur.
      }
    }
  }

  it should "fail if get fails, and give the key of failure" in {
    val dMapService = DataMapService
    val testMapMap: HeapMap[String, DataMap[String, Any]] = new HeapMap[String, DataMap[String, Any]]
    val testmap: HeapMap[String, Any] = new HeapMap[String, Any]
    testMapMap.put("test", testmap)
    val testActorMap: HeapMap[String, ActorRef] = new HeapMap[String, ActorRef]
    testActorMap.put("test", system.actorOf(Props(new DataMapActor(testmap)), "testDMapActor"))
    val testRoute: server.Route = dMapService.getCommand(testActorMap)
    Get("/get?key=failure&dataMap=test") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": false, \"dataMap\": \"test\", \"key\": \"failure\" }"
    }
    testActorMap.getAllKeys.foldLeft(){
      (_, key) => testActorMap.get(key) match{
        case Some(aRef) => system.stop(aRef)
        case None => () //Should never occur.
      }
    }
  }

  it should "get a list of values when asked" in {
    val dMapService = DataMapService
    val testMapMap: HeapMap[String, DataMap[String, Any]] = new HeapMap[String, DataMap[String, Any]]
    val testmap: HeapMap[String, Any] = new HeapMap[String, Any]
    testMapMap.put("test", testmap)
    val testActorMap: HeapMap[String, ActorRef] = new HeapMap[String, ActorRef]
    testActorMap.put("test", system.actorOf(Props(new DataMapActor(testmap)), "testDMapActor"))
    val testRoute: server.Route = dMapService.getCommand(testActorMap)
    Post("/put", "{ \"key\": \"test\", \"dataMap\": \"test\", \"value\": \"test\" }") ~> testRoute
    Post("/put", "{ \"key\": \"test2\", \"dataMap\": \"test\", \"value\": \"test\" }") ~> testRoute
    Post("/put", "{ \"key\": \"test3\", \"dataMap\": \"test\", \"value\": \"test\" }") ~> testRoute
    Post("/put", "{ \"key\": \"test\", \"dataMap\": \"test\", \"value\": \"?\" }") ~> testRoute
    Get("/getKeys?dataMap=test") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"dataMap\": \"test\", \"keys\": [ \"test\", \"test2\", \"test3\" ] }"
    }
    testActorMap.getAllKeys.foldLeft(){
      (_, key) => testActorMap.get(key) match{
        case Some(aRef) => system.stop(aRef)
        case None => () //Should never occur.
      }
    }
  }

  it should "allow non-string values" in {
    val dMapService = DataMapService
    val testMapMap: HeapMap[String, DataMap[String, Any]] = new HeapMap[String, DataMap[String, Any]]
    val testmap: HeapMap[String, Any] = new HeapMap[String, Any]
    testMapMap.put("test", testmap)
    val testActorMap: HeapMap[String, ActorRef] = new HeapMap[String, ActorRef]
    testActorMap.put("test", system.actorOf(Props(new DataMapActor(testmap)), "testDMapActor"))
    val testRoute: server.Route = dMapService.getCommand(testActorMap)
    Post("/put", "{ \"key\": \"test\", \"value\": 9, \"dataMap\": \"test\"  }") ~> testRoute
    Post("/put", "{ \"key\": \"test2\", \"value\": [ \"a\", \"b\", \"c\" ], \"dataMap\": \"test\" }") ~> testRoute
    Get("/get?key=test&dataMap=test") ~> testRoute ~> check{
      responseAs[String] shouldEqual "{ \"succ\": true, \"dataMap\": \"test\", \"key\": \"test\", \"value\": 9.0 }"
    }
    Get("/get?key=test2&dataMap=test") ~> testRoute ~> check{
      responseAs[String] shouldEqual "{ \"succ\": true, \"dataMap\": \"test\", \"key\": \"test2\", \"value\": [ \"a\", \"b\", \"c\" ] }"
    }
    testActorMap.getAllKeys.foldLeft(){
      (_, key) => testActorMap.get(key) match{
        case Some(aRef) => system.stop(aRef)
        case None => () //Should never occur.
      }
    }
  }

  it should "be able to handle more than one DataMap at a time" in {
    val dMapService = DataMapService
    val testMapMap: HeapMap[String, DataMap[String, Any]] = new HeapMap[String, DataMap[String, Any]]
    val testmap: HeapMap[String, Any] = new HeapMap[String, Any]
    val testmap2: HeapMap[String, Any] = new HeapMap[String, Any]
    testMapMap.put("test", testmap)
    testMapMap.put("test2", testmap2)
    val testActorMap: HeapMap[String, ActorRef] = new HeapMap[String, ActorRef]
    testActorMap.put("test", system.actorOf(Props(new DataMapActor(testmap)), "testDMapActor"))
    testActorMap.put("test2", system.actorOf(Props(new DataMapActor(testmap2)), "testDMapActor2"))
    val testRoute: server.Route = dMapService.getCommand(testActorMap)
    Post("/put", "{ \"key\": \"test\", \"value\": \"Datamap 1\", \"dataMap\": \"test\" }") ~> testRoute
    Post("/put", "{ \"key\": \"test\", \"value\": \"Datamap 2\", \"dataMap\": \"test2\" }") ~> testRoute
    Get("/get?key=test&dataMap=test") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"dataMap\": \"test\", \"key\": \"test\", \"value\": \"Datamap 1\" }"
    }
    Get("/get?key=test&dataMap=test2") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"dataMap\": \"test2\", \"key\": \"test\", \"value\": \"Datamap 2\" }"
    }
    testActorMap.getAllKeys.foldLeft(){
      (_, key) => testActorMap.get(key) match{
        case Some(aRef) => system.stop(aRef)
        case None => () //Should never occur.
      }
    }
  }

  it should "allow you to add a DataMap to the service temporarily" in {
    val dMapService = DataMapService
    val testMapMap: HeapMap[String, DataMap[String, Any]] = new HeapMap[String, DataMap[String, Any]]
    val testmap: HeapMap[String, Any] = new HeapMap[String, Any]
    testMapMap.put("test", testmap)
    val testActorMap: HeapMap[String, ActorRef] = new HeapMap[String, ActorRef]
    testActorMap.put("test", system.actorOf(Props(new DataMapActor(testmap)), "testDMapActor"))
    val testRoute: server.Route = dMapService.getCommand(testActorMap)
    Post("/add", "{ \"name\": \"test2\", \"type\": \"Heap\" }") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"dataMap\": \"test2\", \"perm\": false }"
    }
    Post("/put", "{ \"key\": \"test\", \"value\": \"!\", \"dataMap\": \"test2\", \"perm\": false }") ~> testRoute
    Get("/get?key=test&dataMap=test2") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"dataMap\": \"test2\", \"key\": \"test\", \"value\": \"!\" }"
    }
    testActorMap.getAllKeys.foldLeft(){
      (_, key) => testActorMap.get(key) match{
        case Some(aRef) => system.stop(aRef)
        case None => () //Should never occur.
      }
    }
  }

  it should "not allow you to add a DataMap to the service if it already exists" in {
    val dMapService = DataMapService
    val testMapMap: HeapMap[String, DataMap[String, Any]] = new HeapMap[String, DataMap[String, Any]]
    val testmap: HeapMap[String, Any] = new HeapMap[String, Any]
    testMapMap.put("test", testmap)
    val testActorMap: HeapMap[String, ActorRef] = new HeapMap[String, ActorRef]
    testActorMap.put("test", system.actorOf(Props(new DataMapActor(testmap)), "testDMapActor"))
    val testRoute: server.Route = dMapService.getCommand(testActorMap)
    Post("/add", "{ \"name\": \"test\", \"type\": \"Heap\" }") ~> testRoute ~> check{
      responseAs[String] shouldEqual "{ \"succ\": false, \"dataMap\": \"test\" }"
    }
  }
}

