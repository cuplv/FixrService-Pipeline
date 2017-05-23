/**
  * Created by chanceroberts on 5/22/17.
  */
package edu.colorado.plv.fixrservice.pipeline

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.{Http, server}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.joda.time.Seconds

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.Success
import scala.util.parsing.json.JSON


class DataMapService {
  implicit val system = ActorSystem("System")
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(5 seconds)
  def getCommand(aRef: ActorRef): server.Route = {
    post {
      path("put") {
        entity(as[String]) {
          queryStr =>
            JSON.parseFull(queryStr) match{
              case Some(map: Map[String, Any]) =>
                aRef ! map
                complete("")
              case Some(list: List[Any]) =>
                if (list.length > 1){
                  val map = Map.empty + ("key" -> list.head) + ("value" -> list(1))
                  aRef ! map
                  complete("")
                } else {
                  complete("{ \"succ\": false }")
                }//key is first thing in list, value is second thing in list
              case _ => complete("{ \"succ\": false }") //{'succ': false}
            }
        }
      }
    }
    get {
      path("get") {
        entity(as[String]) {
          queryStr =>
            JSON.parseFull(queryStr) match {
              case Some(map: Map[String, Any]) =>
                val res: Future[Any] = aRef ? map
                onComplete(res) {
                  case Success(msg: String) => complete(msg)
                  case _ => complete("{ \"succ\": false }")
                }

              case Some(list: List[Any]) =>
                if (list.nonEmpty){
                  val map = Map.empty[String, Any] + ("key" -> list.head)
                  val res: Future[Any] = aRef ? map
                  onComplete(res) {
                    case Success(msg: String) => complete(msg)
                    case _ => complete("{ \"succ\": false }")
                  }
                } else {
                  complete("{ \"succ\": false }")
                } //key is first list value
              case _ => complete("{ \"succ\": false }") //{'succ': false}
            }
        }
      }
    }
  }

  def main(args: Array[String]) {
    def possibly(args: Array[String], index: Int, default: String): String = {
      if (args.length > index) args(index) else default
    }
    val dMap = {
      if (args.length == 0){
        new HeapMap[String, String]
      } else args(0) match{
        case "MongoDB" =>
          if (args.length > 2){
            val dBaseName = args(1)
            val collName = args(2)
            //To do: Add username, password, location, port.
            new MongoDBMap[String, String](dBaseName, collName,
              possibly(args, 3, "localhost"), possibly(args, 4, "27017"),
              possibly(args, 5, ""), possibly(args, 6, ""))
          } else {
            new HeapMap[String, String]
          }
        case "Heap" => new HeapMap[String, String]
        case "Null" => new NullMap[String, String]
      }
    }
    val dMapActor = system.actorOf(Props(new DataMapActor(dMap)), "dMapActor")
    val route = getCommand(dMapActor)
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

class DataMapActor(dataMap: DataMap[String, String]) extends Actor{
  val dMap: DataMap[String, String] = dataMap

  def receive = {
    case msg: Map[String, Any] => msg("key") match{
      case Some(key) => msg("value") match{
        case Some(value) => dMap.put(key.toString, value.toString)
        case None => dMap.get(key.toString) match{
          case Some(value) => sender() ! "{ \"succ\": true, \"key\": \""  + key.toString + "\", \"value\": \"" + value + "\" }"
          case None => sender() ! "{ \"succ\": false, \"key\": \"" + key.toString + "\" }"
        }
      }
      case _ => sender() ! """{ "succ": false, "key": "" }"""
    }
  }
}
