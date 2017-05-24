/**
  * Created by chanceroberts on 5/23/17.
  */
import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.model.FormData
import akka.http.scaladsl.server
import org.scalatest._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import edu.colorado.plv.fixrservice.pipeline._

class DataMapServiceTest extends FlatSpec with Matchers with ScalatestRouteTest {
  "DataMapService" should " work with post /put and get /get with basic query" in {
    val dMapService = new DataMapService
    val testMap: HeapMap[String, String] = new HeapMap[String, String]
    val testDMapActor: ActorRef = system.actorOf(Props(new DataMapActor(testMap)), "testMapActor")
    val testRoute: server.Route = dMapService.getCommand(testDMapActor)
    Post("/put", "{ \"key\": \"test\", \"value\": \"Hello, world!\" }") ~> testRoute
    Get("/get?key=test") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"key\": \"test\", \"value\": \"Hello, world!\" }"
    }
    system.stop(testDMapActor)
  }

  it should "fail if get fails, and give the key of failure" in {
    val dMapService = new DataMapService
    val testMap: HeapMap[String, String] = new HeapMap[String, String]
    val testDMapActor: ActorRef = system.actorOf(Props(new DataMapActor(testMap)), "testMapActor2")
    val testRoute: server.Route = dMapService.getCommand(testDMapActor)
    Get("/get?key=failure") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": false, \"key\": \"failure\" }"
    }
    system.stop(testDMapActor)
  }

  it should "get a list of values when asked" in {
    val dMapService = new DataMapService
    val testMap: HeapMap[String, String] = new HeapMap[String, String]
    val testDMapActor: ActorRef = system.actorOf(Props(new DataMapActor(testMap)), "testMapActor2")
    val testRoute: server.Route = dMapService.getCommand(testDMapActor)
    Post("/put", "{ \"key\": \"test\", \"value\": \"test\" }") ~> testRoute
    Post("/put", "{ \"key\": \"test2\", \"value\": \"test\" }") ~> testRoute
    Post("/put", "{ \"key\": \"test3\", \"value\": \"test\" }") ~> testRoute
    Post("/put", "{ \"key\": \"test\", \"value\": \"?\" }") ~> testRoute
    Get("/getKeys") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"keys\": [ \"test\", \"test2\", \"test3\" ] }"
    }
    system.stop(testDMapActor)
  }
}

