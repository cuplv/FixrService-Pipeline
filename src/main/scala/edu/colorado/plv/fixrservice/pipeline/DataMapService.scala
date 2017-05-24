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

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.StdIn
import scala.util.Success
import scala.util.parsing.json.JSON
import com.typesafe.config.{Config, ConfigException, ConfigFactory}


class DataMapService {
  implicit val system = ActorSystem("System")
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(5 seconds)
  def getCommand(aRef: ActorRef): server.Route = {

    post {
      /*path("put") {
        formField("key", "value") { (key, value) =>
          aRef ! (key, value)
          complete("")
        }*/
        /*parameters("key", "value") { (key, value) =>
          aRef ! (key, value)
          complete("")
        }*/
      entity(as[String]) {
        queryStr =>
          JSON.parseFull(queryStr) match{
            case Some(map: Map[String, Any]) =>
              val res = aRef ? map
              onComplete(res){
                case Success(x: String) => complete(x)
                case _ => complete("{ \"succ\": false }")
              }
            case Some(list: List[Any]) =>
              if (list.length > 1){
                val map = Map.empty + ("key" -> list.head) + ("value" -> list(1))
                aRef ! map
                complete("")
              } else {
                complete("{ \"succ\": false }")
              }//key is first thing in list, value is second thing in list
            case _ => complete("{ \"succ\": false }")
          }
      }
    } ~
    get {
      path("get") {
        parameter("key") { key =>
          val res: Future[Any] = aRef ? key
          onComplete(res){
            case Success(msg: String) => complete(msg)
            case _ => complete("{ \"succ\": false, \"key\": \"" + key + "\" }")
          }
        } ~
        path("put") {
          parameters("key", "value") { (key, value) =>
            aRef ! (key, value)
            complete("")
          }
        }
        /*entity(as[String]) {
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
            complete(queryStr)

        }
        */
      }
    }
  }

  def main(args: Array[String]) {
    def possibly(args: Array[String], index: Int, default: String): String = {
      if (args.length > index) args(index) else default
    }
    def possiblyConfig(config: Config, field: String, default: String): String = {
      try{
        config.getString(field)
      } catch{
        case ceM: ConfigException.Missing => default
        case ceWT: ConfigException.WrongType => default
      }
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
        case confFile =>
          val config = ConfigFactory.load(confFile)
          possiblyConfig(config, "DatabaseType", "Heap") match{
            case "MongoDB" =>
              val dBaseName = possiblyConfig(config, "Database", "test")
              val collName = possiblyConfig(config, "Collection", "coll")
              val ip = possiblyConfig(config, "IP", "localhost")
              val port = possiblyConfig(config, "Port", "27017")
              val userName = possiblyConfig(config, "Username", "")
              val password = possiblyConfig(config, "Password", "")
              new MongoDBMap[String, String](dBaseName, collName, ip, port, userName, password)
            case "Heap" => new HeapMap[String, String]
            case "Null" => new NullMap[String, String]
          }
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
    case msg: Map[String, Any] => try {
      msg("key") match {
        case key: String => msg("value") match {
          case value: String =>
            dMap.put(key.toString, value.toString)
            sender() ! ""
          case _ => sender() ! "{ \"succ\": false, \"key\": \"" + key.toString + "\" }"
        }
        case _ => sender() ! """{ "succ": false }"""
      }
    } catch {
      case nsee: NoSuchElementException => sender() ! """{ "succ": false, "key": "" }"""
    }
    case key: String => dMap.get(key) match{
      case Some(value) => sender() ! "{ \"succ\": true, \"key\": \""  + key + "\", \"value\": \"" + value + "\" }"
      case None => sender() ! "{ \"succ\": false, \"key\": \"" + key + "\" }"
    }
    case (key: String, value: String) =>
      dMap.put(key, value)
  }
}
