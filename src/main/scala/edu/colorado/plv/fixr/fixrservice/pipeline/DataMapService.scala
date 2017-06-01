/**
  * Created by chanceroberts on 5/22/17.
  */
package edu.colorado.plv.fixr.fixrservice.pipeline

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


object DataMapService {
  implicit val system = ActorSystem("System")
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(5 seconds)
  def getCommand(aMapRef: DataMap[String, ActorRef]): server.Route = {

    post {
      path("put") {
        /*parameters("key", "value") { (key, value) =>
          aRef ! (key, value)
          complete("")
        }*/
        entity(as[String]) {
          queryStr =>
            JSON.parseFull(queryStr) match {
              case Some(map: Map[String @ unchecked, Any @ unchecked]) => map("dataMap") match {
                case Some(x) =>
                  aMapRef.get(map("dataMap").toString) match{
                    case Some(a: ActorRef) =>
                      val res = a ? map
                      onComplete (res) {
                        case Success (x: String) => complete (x)
                        case _ => complete ("{ \"succ\": false }")
                      }
                    case None => complete("{ \"succ\": false }")
                  }
              }
              case Some(list: List[Any]) =>
                if (list.length > 1) {
                  val map = Map.empty + ("key" -> list.head) + ("value" -> list(1))
                  aMapRef.get(list(2).toString) match{
                    case Some(a: ActorRef) => a ! map
                      complete("")
                    case None => complete("{ \"succ\": false }")
                  }
                } else {
                  complete("{ \"succ\": false }")
                } //key is first thing in list, value is second thing in list
              case _ => complete("{ \"succ\": false }")
            }
        }
      }
    } ~
    get {
      path("getKeys") {
        parameter("dataMap") { dataMap =>
          aMapRef.get(dataMap) match {
            case Some(aRef: ActorRef) => val res: Future[Any] = aRef ? List.empty[String]
              onComplete(res) {
                case Success(fields: List[String]) =>
                  val fullString = {
                    if (fields.nonEmpty) {
                      val mostOfString = fields.foldLeft("{ \"succ\": true, \"keys\": [ ") {
                        case (str, key) => str + "\"" + key + "\", "
                      }
                      mostOfString.substring(0, mostOfString.length - 2) + " ] }"
                    } else {
                      "{ \"succ\": true, \"keys\": [] }"
                    }
                  }
                  complete(fullString)
                case _ => complete("{ \"succ\": false }")
              }
            case _ => complete("{ \"succ\": false }")
          }
        }
      } ~
      path("get") {
        parameters("key", "dataMap") { (key, dataMap) =>
          aMapRef.get(dataMap) match {
            case Some(aRef: ActorRef) => val res: Future[Any] = aRef ? key
              onComplete(res){
                case Success(msg: String) => complete(msg)
                case _ => complete("{ \"succ\": false, \"key\": \"" + key + "\" }")
              }
            case None => complete("{ \" succ\": false, \"key\": \"" + key + "\" }")
          }

        }
      }
    }
  }

  def main(args: Array[String]) {
    def possibly(args: Array[String], index: Int, default: String): String = {
      if (args.length > index) args(index) else default
    }
    def possiblyConfig[A](config: Config, field: String, default: A): A = {
      try{
        default match {
          case _: String => config.getString(field).asInstanceOf[A]
          case _: Int => config.getInt(field).asInstanceOf[A]
          case _: Float => config.getDouble(field).asInstanceOf[A]
          case _: Double => config.getDouble(field).asInstanceOf[A]
          case _: Boolean => config.getBoolean(field).asInstanceOf[A]
          case _: Object => config.getObject(field).asInstanceOf[A]
          case _ => default
        }
      } catch{
        case ceM: ConfigException.Missing => default
        case ceWT: ConfigException.WrongType => default
      }
    }

    def possibly2Config[A](config: Config, field: String, field2: String, default: A): A = {
      possiblyConfig(config, field, default) match{
        case x if x == default => possiblyConfig(config, field2, default)
        case x => x
      }
    }

    val (dataMapMap: DataMap[String, DataMap[String, Any]], actorMap: DataMap[String, ActorRef]) = {
      if (args.length == 0){
        val hMap = new HeapMap[String, HeapMap[String, Any]]
        val aMap = new HeapMap[String, ActorRef]
        val hMap1 = new HeapMap[String, Any]
        hMap.put("Default", hMap1)
        aMap.put("Default", system.actorOf(Props(new DataMapActor(hMap1)), "DefaultDMapActor"))
        (hMap, aMap)
      } else args(0) match{
        case confFile =>
          val config = ConfigFactory.load(confFile)
          val numOfDataMaps = possiblyConfig(config, "NumOfDatabases", 1)
          def addADatabase(dataMapNumber: Int, currMap: DataMap[String, DataMap[String, Any]], actorMap: DataMap[String, ActorRef]):
            (DataMap[String, DataMap[String, Any]], DataMap[String, ActorRef]) = dataMapNumber match{
            case x if x > numOfDataMaps => (currMap, actorMap)
            case x =>
              val dataNumberString = dataMapNumber.toString
              val dataMapID = "Datamap"+dataNumberString
              val dataMapName = possiblyConfig(config, dataMapID+"ID", "DataMap"+dataNumberString)
              val newDataMap = possibly2Config(config, dataMapID+"DatabaseType", dataMapName+"DatabaseType", "Heap") match{
                case "MongoDB" =>
                  val dBaseName = possibly2Config(config, dataMapID+"Database", dataMapName+"Database", "test")
                  val collName = possibly2Config(config, dataMapID+"Collection", dataMapName+"Collection", "coll")
                  val ip = possibly2Config(config, dataMapID+"IP", dataMapName+"IP", "localhost")
                  val port = possibly2Config(config, dataMapID+"Port", dataMapName+"Port", "27017")
                  val userName = possibly2Config(config, dataMapID+"Username", dataMapName+"Username", "")
                  val password = possibly2Config(config, dataMapID+"Password", dataMapName+"Password", "")
                  new MongoDBMap[String, Any](dBaseName, collName, ip, port, userName, password)
                case "Solr" =>
                  val collName = possibly2Config(config, dataMapID+"Collection", dataMapName+"Collection", "gettingstarted")
                  val fName = possibly2Config(config, dataMapID+"Field", dataMapName+"Field", "value")
                  val ip = possibly2Config(config, dataMapID+"IP", dataMapName+"IP", "localhost")
                  val port = possibly2Config(config, dataMapID+"Port", dataMapName+"Port", "8983")
                  new SolrMap[String, Any](collName, fName, ip, port)
                case "Heap" => new HeapMap[String, Any]
                case "Null" => new NullMap[String, Any]
              }
              currMap.put(dataMapName, newDataMap)
              actorMap.put(dataMapName, system.actorOf(Props(new DataMapActor(newDataMap)), dataMapName+"DMapActor"))
              (currMap, actorMap)
          }
          addADatabase(1, new HeapMap[String, DataMap[String, Any]], new HeapMap[String, ActorRef])
      }
    }

    /*
    val dMapActor = system.actorOf(Props(new DataMapActor(dMap)), "dMapActor")
    val route = getCommand(dMapActor)
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
      */
  }
}

class DataMapActor(dataMap: DataMap[String, Any]) extends Actor{
  val dMap: DataMap[String, Any] = dataMap

  def receive = {
    case msg: Map[String @ unchecked, Any @ unchecked] => try {
      msg("key") match {
        case key: String => msg("value") match {
          case value: Any =>
            dMap.put(key.toString, value)
            sender() ! ""
          case _ => sender() ! "{ \"succ\": false, \"key\": \"" + key.toString + "\" }"
        }
        case _ => sender() ! """{ "succ": false }"""
      }
    } catch {
      case nsee: NoSuchElementException => sender() ! """{ "succ": false, "key": "" }"""
    }
    case key: String => dMap.get(key) match{
      case Some(s: String) => sender() ! "{ \"succ\": true, \"key\": \""  + key + "\", \"value\": \"" + s + "\" }"
      case Some(l: List[_]) =>
        val start = l.foldLeft("{ \"succ\": true, \"key\": \"" + key + "\", \"value\": [ "){
          case (str, s: String) => str + "\"" + s.toString + "\", "
          case (str, v) => str + v.toString + ", "
        }
        val string = start.substring(0,start.length-2) + " ] }"
        sender() ! string
      case Some(a: Any) => sender() ! "{ \"succ\": true, \"key\": \"" + key + "\", \"value\": " + a.toString + " }"
      case None => sender() ! "{ \"succ\": false, \"key\": \"" + key + "\" }"
    }
    case (key: String, value: String) =>
      dMap.put(key, value)
    case _: List[_] =>
      sender() ! dMap.getAllKeys
  }
}
