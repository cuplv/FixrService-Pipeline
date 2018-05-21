import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.{Http, server}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Future
import scala.io.StdIn

class Termination(bindingFuture: Future[Http.ServerBinding]) extends Actor{
  def receive: Receive = {
    case _ =>
      Thread.sleep(1000)
      GraphExtractionService.terminate(bindingFuture)
      sys.exit(0)
  }
}

/**
  * Created by chanceroberts on 5/16/18.
  */
object GraphExtractionService {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  val config = ConfigFactory.load()
  var terminatingOpt: Option[ActorRef] = None

  def terminate(bindingFuture: Future[Http.ServerBinding]): Unit = {
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

  def terminating: ActorRef = terminatingOpt match {
    case Some(t) => t
    case None => throw new Exception("Terminating Actor is nonexistent.")
  }

  def getRequest: server.Route = {
    post{
      entity(as[String]){
        queryStr =>
          path("extract"){
            complete(GroumCommands.extractGraph(queryStr, config))
          }~
          path("end"){
            println("Time to end this!")
            terminating ! 0
            complete("")
          }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //GroumCommands.setupDocker(config)
    //Set up the actual service.
    val route = getRequest
    val port = args.length match {
      case 0 => config.getInt("port")
      case x =>
        Integer.parseInt(args(0))
    }
    val bindingFuture = akka.http.scaladsl.Http().bindAndHandle(route, "localhost", port)
    terminatingOpt = Some(system.actorOf(Props(classOf[Termination], bindingFuture)))
    val term = StdIn.readLine() // let it run until user presses return
    //GroumCommands.removeDocker()
    if (term != null) {
      bindingFuture
        .flatMap(_.unbind()) // trigger unbinding from the port
        .onComplete(_ => system.terminate()) // and shutdown when done
    }
  }
}
