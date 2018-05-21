import GraphExtractionService.{config, getRequest, system}
import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.io.StdIn
import scalaj.http.Http

/**
  * Created by chanceroberts on 5/17/18.
  */

case class CreateNode(p: String)

class Node extends Actor{
  def receive: Receive = {
    case CreateNode(p) =>
      // GroumCommands.runCommand(Seq("sbt", "run-main", "GraphExtractionService", p))
      GroumCommands.runCommand("sbt", s"runMain GraphExtractionService $p")
    case _ => ()
  }
}


object GraphExtractionFrontend {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
  val config = ConfigFactory.load()
  implicit var nodes: Map[String, Int] = Map()

  def getNextNode: String = {
    val (node, _) = nodes.foldLeft(("", Int.MaxValue)){
      case ((nod, num), (str, a)) => if (a < num) (str, a) else (nod, num)
    }
    node
  }

  def getRequest: server.Route = {
    post{
      entity(as[String]){
        queryStr =>
          path("extract"){
            val nodeToPick = getNextNode
            nodes = nodes + (nodeToPick -> (nodes(nodeToPick) + 1))
            println(Uri(s"http://localhost:$nodeToPick/extract"))
            redirect(Uri(s"http://localhost:$nodeToPick/extract"), StatusCodes.TemporaryRedirect)
          }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    GroumCommands.setupDocker(config)
    val route = getRequest
    val listNodes: List[String] = args.length match{
      case 0 | 1 => config.getStringList("nodes").asScala.toList
      case _ => args.toList.tail
    }
    nodes = listNodes.foldRight(Map[String, Int]()){
      case (node, map) => map + (node -> 0)
    }
    nodes.foreach{
      case (s, _) =>
        val worker = system.actorOf(Props(classOf[Node]))
        worker ! CreateNode(s)
    }
    val port = args.length match {
      case 0 => config.getInt("port")
      case x => Integer.parseInt(args(0))
    }
    val bindingFuture = akka.http.scaladsl.Http().bindAndHandle(route, "localhost", port)
    StdIn.readLine() // let it run until user presses return
    GroumCommands.removeDocker()
    nodes.foreach { case (str, _) =>
      Http(s"http://$str/end").timeout(1000, 3600000).postData("").asString
    }
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete ( _ => system.terminate()) // and shutdown when done
  }
}
