import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.{Http, server}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import spray.json._

import scala.collection.JavaConverters._
import scala.io.StdIn
import scalaj.http.Http
/**
  * Created by chanceroberts on 10/12/17.
  */
object GithubServiceFrontend {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher
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
          path("clone"){
            nodes.foldRight(""){
              case ((str, i), response) =>
                val httpStr = scalaj.http.Http(s"http://$str/clone").timeout(1000, 3600000).postData(queryStr).header("Content-Type", "application/json").asString.body
                val json = httpStr.parseJson
                json.asJsObject.fields.get("status") match {
                  case Some(JsString("error")) | None => httpStr
                  case _ => response
                }
            } match{
              case "" => complete("{ \"status\": \"ok\" }")
              case x => complete(x)
            }
          } ~
          path("pull"){
            nodes.foldRight(""){
              case ((str, i), response) =>
                val httpStr = scalaj.http.Http(s"http://$str/pull").timeout(1000, 3600000).postData(queryStr).header("Content-Type", "application/json").asString.body
                val json = httpStr.parseJson
                json.asJsObject.fields.get("status") match {
                  case Some(JsString("error")) | None => httpStr
                  case _ => response
                }
            } match{
              case "" => complete("{ \"status\": \"ok\" }")
              case x => complete(x)
            }
          } ~
          path("getCommits"){
            val nodeToPick = getNextNode
            nodes = nodes + (nodeToPick -> (nodes(nodeToPick) + 1))
            println(Uri(s"http://$nodeToPick/getCommits"))
            redirect(Uri(s"http://$nodeToPick/getCommits"), StatusCodes.TemporaryRedirect)
          } ~
          path("commitInformation"){
            val nodeToPick = getNextNode
            nodes = nodes + (nodeToPick -> (nodes(nodeToPick) + 1))
            println(Uri(s"http://$nodeToPick/commitInformation"))
            redirect(Uri(s"http://$nodeToPick/commitInformation"), StatusCodes.TemporaryRedirect)
          } ~
          path("getFiles"){
            val nodeToPick = getNextNode
            nodes = nodes + (nodeToPick -> (nodes(nodeToPick) + 1))
            println(s"${Uri(s"http://$nodeToPick/getFiles")}, $queryStr")
            redirect(Uri(s"http://$nodeToPick/getFiles"), StatusCodes.TemporaryRedirect)
          } ~
          path("fileContents"){
            val nodeToPick = getNextNode
            nodes = nodes + (nodeToPick -> (nodes(nodeToPick) + 1))
            println(Uri(s"http://$nodeToPick/fileContents"))
            redirect(Uri(s"http://$nodeToPick/fileContents"), StatusCodes.TemporaryRedirect)
          } ~
          path("patch"){
            val nodeToPick = getNextNode
            nodes = nodes + (nodeToPick -> (nodes(nodeToPick) + 1))
            println(Uri(s"http://$nodeToPick/patch"))
            redirect(Uri(s"http://$nodeToPick/patch"), StatusCodes.TemporaryRedirect)
          } ~
          path("parent"){
            val nodeToPick = getNextNode
            nodes = nodes + (nodeToPick -> (nodes(nodeToPick) + 1))
            println(Uri(s"http://$nodeToPick/parent"))
            redirect(Uri(s"http://$nodeToPick/parent"), StatusCodes.TemporaryRedirect)
          } ~
          path("resetSolr"){
            val nodeToPick = getNextNode
            nodes = nodes + (nodeToPick -> (nodes(nodeToPick) + 1))
            println(Uri(s"http://$nodeToPick/resetSolr"))
            redirect(Uri(s"http://$nodeToPick/resetSolr"), StatusCodes.TemporaryRedirect)
          } ~
          path("doneWithResults"){
            val json = queryStr.parseJson.asJsObject
            json.fields.get("node") match{
              case Some(JsString(node)) => nodes.get(node) match{
                case Some(x) => nodes = nodes + (node -> (x-1))
                case _ => ()
              }
              case _ => ()
            }
            complete("")
          }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val listNodes: List[String] = args.length match{
      case 0 | 1 => config.getStringList("nodes").asScala.toList
      case _ => args.toList.tail
    }
    nodes = listNodes.foldRight(Map[String, Int]()){
      case (node, map) => map + (node -> 0)
    }
    val port = args.length match {
      case 0 => config.getInt("port")
      case x => Integer.parseInt(args(0))
    }
    val route = getRequest
    val bindingFuture = akka.http.scaladsl.Http().bindAndHandle(route, "localhost", port)
    println(s"Github Service Frontend started on port $port!")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
