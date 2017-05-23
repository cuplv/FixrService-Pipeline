/**
  * Created by chanceroberts on 5/22/17.
  */
package edu.colorado.plv.fixrservice.pipeline

import akka.actor.{Actor, ActorSystem}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.{Http, server}
import akka.stream.ActorMaterializer

import scala.io.StdIn
import scala.util.parsing.json.JSON


class DataMapService {

  def getCommand: server.Route = {
    post {
      path("get") {
        entity(as[String]) {
          queryStr =>
            /*val queryMap = parseQuery(queryStr)
            if (queryMap.contains("key")){
              val json = JSON.parse
            } else {

            }*/
            JSON.parseRaw(queryStr) match{
              case Some(jSON) =>

              case None => //{'succ': false}
            }
            ???
        }
      }
      path("set") {
        entity(as[String]) {
          queryStr =>
            JSON.parseRaw(queryStr) match{
              case Some(jSON) =>
                ???
              case None => ???
            }
            ???
        }
      }
    }
    ???
  }

  def main(args: Array[String]) {
    
    implicit val system = ActorSystem("System")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val route = getCommand
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

class DataMapActor(dataMap: DataMap[String, String]) extends Actor{
  val dMap = dataMap

  def receive = {
    case msg => ???
  }
}
