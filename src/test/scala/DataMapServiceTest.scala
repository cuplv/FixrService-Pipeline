/**
  * Created by chanceroberts on 5/23/17.
  */
import akka.actor.{ActorRef, Props}
import akka.http.scaladsl.server
import org.scalatest._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import edu.colorado.plv.fixrservice.pipeline._

class DataMapServiceTest extends FlatSpec with Matchers with ScalatestRouteTest {
  "DataMapService" should " work with post and get" in {
    val dMapService = new DataMapService
    val testMap: HeapMap[String, String] = new HeapMap[String, String]
    val testDMapActor: ActorRef = system.actorOf(Props(new DataMapActor(testMap)), "testMapActor")
    val testRoute: server.Route = dMapService.getCommand(testDMapActor)
    Post("/put?{\"key\":\"test\",\"value\":\"hello,world!\"}") ~> testRoute
    Get("/get?{\"key\":\"test\"}") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"key\": \"test\", \"value\": \"hello,world!\" }"
    }
    system.stop(testDMapActor)
  }

  it should "fail if get fails, and give the key of failure" in {
    val dMapService = new DataMapService
    val testMap: HeapMap[String, String] = new HeapMap[String, String]
    val testDMapActor: ActorRef = system.actorOf(Props(new DataMapActor(testMap)), "testMapActor")
    val testRoute: server.Route = dMapService.getCommand(testDMapActor)
    Get("/get?{\"key\":\"failure\"") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": false, \"key\": \"failure\" }"
    }
    system.stop(testDMapActor)
  }

  it should "work with \"spaces\" (%20)" in {
    val dMapService = new DataMapService
    val testMap: HeapMap[String, String] = new HeapMap[String, String]
    val testDMapActor: ActorRef = system.actorOf(Props(new DataMapActor(testMap)), "testMapActor")
    val testRoute: server.Route = dMapService.getCommand(testDMapActor)
    Post("/put?{%20\"key\":%20\"test\"%20,\"value\":%20\"Hello,%20world!\"%20}") ~> testRoute
    Get("/get?{%20\"key\":%20\"test\"%20}") ~> testRoute ~> check {
      responseAs[String] shouldEqual "{ \"succ\": true, \"key\": \"test\", \"value\": \"Hello, world!\" }"
    }
    system.stop(testDMapActor)
  }
}

