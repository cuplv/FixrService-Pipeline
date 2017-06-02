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

import scala.io.Source
import java.io.PrintWriter


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
              case Some(map: Map[String @ unchecked, Any @ unchecked]) => map.get("dataMap") match {
                case Some(x) =>
                  val dataMap = x.toString
                  aMapRef.get(dataMap) match{
                    case Some(a: ActorRef) =>
                      val res = a ? (map, dataMap)
                      onComplete (res) {
                        case Success (x: String) => complete (x)
                        case _ => complete ("{ \"succ\": false, dataMap: \"" + dataMap + "\" }")
                      }
                    case None => complete("{ \"succ\": false, dataMap: \"" + dataMap + "\" }")
                  }
                case None => complete("{ \"succ\": false }")
              }
              case Some(list: List[Any]) =>
                if (list.length > 2) {
                  val map = Map.empty + ("key" -> list.head) + ("value" -> list(1))
                  aMapRef.get(list(2).toString) match{
                    case Some(a: ActorRef) => a ! (map, list(2).toString)
                      complete("")
                    case None => complete("{ \"succ\": false, \"dataMap\": \"" + list(2).toString + "\" }")
                  }
                } else {
                  complete("{ \"succ\": false }")
                } //key is first thing in list, value is second thing in list, dataMap is third thing in list. (KEY AND DATAMAP HAVE TO BE STRINGS)
              case _ => complete("{ \"succ\": false }")
            }
        }
      } ~
      path("add") {
        entity(as[String]) {
          queryStr =>
            JSON.parseFull(queryStr) match{
              case Some(map: Map[String @ unchecked, Any @ unchecked]) => map.get("name") match{
                case Some(name: String) => aMapRef.get(name) match{
                  case Some(_) => complete("{ \"succ\": false, \"dataMap\": \"" + name + "\" }")
                  case None =>
                    def possiblyInMap(check: String, default: String): String = map.get(check) match{
                      case Some(x) => x.toString
                      case _ => default
                    }
                    val actorRef: ActorRef = map.get("type") match{
                      case Some("Solr") =>
                        val cName = possiblyInMap("collection", "gettingstarted")
                        val fName = possiblyInMap("field", "value")
                        val IP = possiblyInMap("IP", "localhost")
                        val port = possiblyInMap("port", "8983")
                        system.actorOf(Props(new DataMapActor(new SolrMap[String, Any](cName, fName, IP, port))), name+"DMapActor")
                      case Some("MongoDB") =>
                        val IP = possiblyInMap("IP", "localhost")
                        val port = possiblyInMap("port", "27017")
                        val dBName = possiblyInMap("database", "test")
                        val cName = possiblyInMap("collection", "coll")
                        val uName = possiblyInMap("username", "")
                        val psswrd = possiblyInMap("password", "")
                        system.actorOf(Props(new DataMapActor(new MongoDBMap[String, Any](dBName, cName, IP, port, uName, psswrd))))
                      case Some("Heap") => system.actorOf(Props(new DataMapActor(new HeapMap[String, Any])), name+"DMapActor")
                      case Some("Null") => system.actorOf(Props(new DataMapActor(new NullMap[String, Any])), name+"DMapActor")
                      case _ => system.actorOf(Props(new DataMapActor(new HeapMap[String, Any])), name+"DMapActor")
                    }
                    aMapRef.put(name, actorRef)
                    map.get("perm") match{
                      case Some(true) =>
                        try {
                          //Load the config file first.
                          val confFile = Source.fromFile("").getLines()
                          val wrtFile = new PrintWriter("")
                          val databaseNum: Int = confFile.foldLeft(0){
                            case (num, str) =>
                              if (str.substring(0, 17).equals("NumOfDatabases = ")){
                                val formerNumOfDatabases = str.substring(17).toInt
                                wrtFile.write(str.substring(0, 17)+(formerNumOfDatabases+1).toString)
                                formerNumOfDatabases+1
                              } else{
                                wrtFile.write(str)
                                num
                              }
                          }
                          val dataMapID: String = "DataMap"+databaseNum.toString
                          wrtFile.write(dataMapID+"ID = \""+name+"\"")
                          map.get("type") match{
                            case Some("Solr") =>
                              wrtFile.write(dataMapID+"Type = \"Solr\"")
                              wrtFile.write(dataMapID+"Collection = \""+possiblyInMap("coll", "gettingstarted"))
                              wrtFile.write(dataMapID+"Field = \""+possiblyInMap("field", "value"))
                              wrtFile.write(dataMapID+"IP = \""+possiblyInMap("IP","localhost")+"\"")
                              wrtFile.write(dataMapID+"Port = \""+possiblyInMap("port","8983")+"\"")
                            case Some("MongoDB") =>
                              wrtFile.write(dataMapID+"Type = \"MongoDB\"")
                              wrtFile.write(dataMapID+"Database = \""+possiblyInMap("database", "test"))
                              wrtFile.write(dataMapID+"Collection = \""+possiblyInMap("collection", "coll"))
                              wrtFile.write(dataMapID+"IP = \""+possiblyInMap("IP","localhost")+"\"")
                              wrtFile.write(dataMapID+"Port = \""+possiblyInMap("port","8983")+"\"")
                              wrtFile.write(dataMapID+"Username = \""+possiblyInMap("username", "")+"\"")
                              wrtFile.write(dataMapID+"Password = \""+possiblyInMap("password", "")+"\"")
                            case Some("Null") => wrtFile.write(dataMapID+"Type = \"Null\"")
                            case _ => wrtFile.write(dataMapID+"Type = \"Heap\"")
                          }
                          wrtFile.close()
                          complete("{ \"succ\": true, \"dataMap\": \"" + name + "\", \"perm\": true }")
                        } catch{
                          case _: Exception => complete("{ \"succ\": false }")
                        }
                      case _ => complete("{ \"succ\": true, \"dataMap\": \"" + name + "\", \"perm\": false }")
                    }

                }
                case _ => complete("{ \"succ\": false }")
              }
              case _ => complete("{ \"succ\": false }")
            }
        }
      }
    } ~
    get {
      path("getKeys") {
        parameter("dataMap") { dataMap: String =>
          aMapRef.get(dataMap) match {
            case Some(aRef: ActorRef) => val res: Future[Any] = aRef ? List.empty[String]
              onComplete(res) {
                case Success(fields: List[String]) =>
                  val fullString = {
                    if (fields.nonEmpty) {
                      val mostOfString = fields.foldLeft("{ \"succ\": true, \"dataMap\": \"" + dataMap + "\", \"keys\": [ ") {
                        case (str, key) => str + "\"" + key + "\", "
                      }
                      mostOfString.substring(0, mostOfString.length - 2) + " ] }"
                    } else {
                      "{ \"succ\": true, \"dataMap\": \"" + dataMap + "\", \"keys\": [] }"
                    }
                  }
                  complete(fullString)
                case _ => complete("{ \"succ\": false, \"dataMap\": \"" + dataMap + "\" }")
              }
            case _ => complete("{ \"succ\": false, \"dataMap\": \"" + dataMap + "\" }")
          }
        }
      } ~
      path("get") {
        parameters("key", "dataMap") { (key, dataMap) =>
          aMapRef.get(dataMap) match {
            case Some(aRef: ActorRef) => val res: Future[Any] = aRef ? (key, dataMap)
              onComplete(res){
                case Success(msg: String) => complete(msg)
                case _ => complete("{ \"succ\": false, \"dataMap: \"" + dataMap + "\", \"key\": \"" + key + "\" }")
              }
            case None => complete("{ \" succ\": false, \"dataMap: \"" + dataMap + "\", \"key\": \"" + key + "\" }")
          }

        }
      }
    }
  }

  def main(args: Array[String]) {
    /*def possibly(args: Array[String], index: Int, default: String): String = {
      if (args.length > index) args(index) else default
    }*/
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
        case _: ConfigException.Missing => default
        case _: ConfigException.WrongType => default
      }
    }

    def possibly2Config[A](config: Config, field: String, field2: String, default: A): A = {
      possiblyConfig(config, field, default) match{
        case x if x == default => possiblyConfig(config, field2, default)
        case x => x
      }
    }

    val actorMap: DataMap[String, ActorRef] = {
      if (args.length == 0){
        val hMap = new HeapMap[String, HeapMap[String, Any]]
        val aMap = new HeapMap[String, ActorRef]
        val hMap1 = new HeapMap[String, Any]
        hMap.put("Default", hMap1)
        aMap.put("Default", system.actorOf(Props(new DataMapActor(hMap1)), "DefaultDMapActor"))
        aMap
      } else args(0) match{
        case confFile =>
          val config = ConfigFactory.load(confFile)
          val numOfDataMaps = possiblyConfig(config, "NumOfDatabases", 1)
          def addADatabase(dataMapNumber: Int, actorMap: DataMap[String, ActorRef]): DataMap[String, ActorRef] = dataMapNumber match{
            case x if x > numOfDataMaps => actorMap
            case x =>
              val dataNumberString = dataMapNumber.toString
              val dataMapID = "DataMap"+dataNumberString
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
                case _ => new HeapMap[String, Any]
              }
              actorMap.put(dataMapName, system.actorOf(Props(new DataMapActor(newDataMap)), dataMapName+"DMapActor"))
              addADatabase(x+1, actorMap)
          }
          addADatabase(1, new HeapMap[String, ActorRef])
      }
    }
    val route = getCommand(actorMap)
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

  }
}

class DataMapActor(dataMap: DataMap[String, Any]) extends Actor{
  val dMap: DataMap[String, Any] = dataMap

  def receive = {
    case (msg: Map[String @ unchecked, Any @ unchecked], dataMap: String) => try {
      msg("key") match {
        case key: String => msg("value") match {
          case value: Any =>
            dMap.put(key.toString, value)
            sender() ! ""
          case _ => sender() ! "{ \"succ\": false, \"dataMap\": \"" + dataMap + "\", \"key\": \"" + key.toString + "\" }"
        }
        case _ => sender() ! "{ \"succ\": false, \"dataMap\": \"" + dataMap + "\" }"
      }
    } catch {
      case _: NoSuchElementException => sender() ! "{ \"succ\": false, \"dataMap\": " + dataMap + "\", \"key\": \"\" }"
    }
    case (key: String, dataMap: String) => dMap.get(key) match{
      case Some(s: String) => sender() ! "{ \"succ\": true, \"dataMap\": \"" + dataMap + "\", \"key\": \""  + key + "\", \"value\": \"" + s + "\" }"
      case Some(l: List[_]) =>
        val start = l.foldLeft("{ \"succ\": true, \"dataMap\": \"" + dataMap + "\", \"key\": \"" + key + "\", \"value\": [ "){
          case (str, s: String) => str + "\"" + s.toString + "\", "
          case (str, v) => str + v.toString + ", "
        }
        val string = start.substring(0,start.length-2) + " ] }"
        sender() ! string
      case Some(a: Any) => sender() ! "{ \"succ\": true, \"dataMap\": \"" + dataMap + "\", \"key\": \"" + key + "\", \"value\": " + a.toString + " }"
      case None => sender() ! "{ \"succ\": false, \"dataMap\": \"" + dataMap + "\", \"key\": \"" + key + "\" }"
    }
    case (key: String, value: String, _: String) =>
      dMap.put(key, value)
    case _: List[_] =>
      sender() ! dMap.getAllKeys
  }
}
