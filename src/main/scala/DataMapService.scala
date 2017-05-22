/**
  * Created by chanceroberts on 5/22/17.
  */
package edu.colorado.plv.fixr.pipeline

import akka.actor.ActorSystem
import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.{Http, server}
import akka.http.scaladsl.server.Directives._
import akka.stream.{ActorMaterializer, Materializer}

import scala.io.StdIn
import scala.util.parsing.json.JSON


class DataMapService {
  implicit val system = ActorSystem("System")
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

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
    val route = getCommand
    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}

class DataMapActor extends Actor{
  def receive = {
    case msg => ???
  }
}
