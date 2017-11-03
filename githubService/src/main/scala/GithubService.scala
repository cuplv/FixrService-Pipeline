import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.{Http, server}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import spray.json._

import scala.io.StdIn
/**
  * Created by chanceroberts on 9/26/17.
  */
object GithubService {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher


  def getRequest(config: Config): server.Route = {
    post{
      entity(as[String]){
        queryStr =>
          path("clone") {
            try{
              val json = queryStr.parseJson.asJsObject
              json.fields.get("repo") match{
                case Some(j: JsString) =>
                  val cloneStuff = GithubCommands.clone(j.value, config).prettyPrint
                  complete(cloneStuff)
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("clone", "{\"repo\": \"user/repo\"}").getMessage))).prettyPrint)
              }
            } catch{
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          } ~
          path("pull"){
            try {
              val json = queryStr.parseJson.asJsObject
              json.fields.get("repo") match {
                case Some(j: JsString) =>
                  val pullStuff = GithubCommands.pull(j.value, config).prettyPrint
                  complete(pullStuff) // TODO: Add stuff here.
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("pull", "{\"repo\": \"user/repo\"}").getMessage))).prettyPrint)
              }
            } catch{
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          } ~
          path("getCommits") {
            try{
              val json = queryStr.parseJson.asJsObject
              json.fields.get("repo") match {
                case Some(j: JsString) =>
                  val lastGet = json.fields.get("sinceLast") match{
                    case Some(JsTrue) => true
                    case _ => false
                  }
                  val sinceDate = json.fields.get("since") match{
                    case Some(jn: JsNumber) => jn.toString()
                    case Some(JsString(js)) => js
                    case _ => ""
                  }
                  val pattern: Option[String] = json.fields.get("pattern") match{
                    case Some(JsString(s)) => Some(s)
                    case _ => None
                  }
                  val getCommits = GithubCommands.getCommits(j.value, config, lastGet, sinceDate, pattern).prettyPrint
                  complete(getCommits)
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("getCommits", "{\"repo\": \"user/repo\" (, \"pattern\": \"regex\", \"sinceLast\": true, \"since\": unixDate/\"date\")}").getMessage))).prettyPrint)
              }
            } catch {
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          } ~
          path("commitInformation") {
            try{
              val json = queryStr.parseJson.asJsObject()
              (json.fields.get("repo"), json.fields.get("commit")) match{
                case (Some(repo: JsString), Some(commit: JsString)) =>
                  val extractCommits = GithubCommands.extractCommit(repo.value, commit.value, config).prettyPrint
                  complete(extractCommits)
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("commitInformation", "{\"repo\": \"user/repo\", \"commit\": \"commitHash\"}").getMessage))).prettyPrint)
              }
            } catch{
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          } ~
          path("getFiles"){
            try {
              val json = queryStr.parseJson.asJsObject()
              (json.fields.get("repo"), json.fields.get("commit")) match {
                case (Some(repo: JsString), Some(commit: JsString)) =>
                  val pattern = json.fields.get("pattern") match {
                    case Some(JsString(s)) => Some(s)
                    case _ => None
                  }
                  val getFiles = GithubCommands.getFiles(repo.value, commit.value, pattern, config).prettyPrint
                  complete(getFiles)
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("getFiles", "{\"repo\": \"user/repo\", \"commit\": \"commitHash\"}").getMessage))).prettyPrint)
              }
            } catch{
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          } ~
          path("fileContents") {
            try {
              val json = queryStr.parseJson.asJsObject()
              (json.fields.get("repo"), json.fields.get("commit"), json.fields.get("file")) match {
                case (Some(repo: JsString), Some(commit: JsString), Some(file: JsString)) =>
                  val getFileContents = GithubCommands.getFileContents(repo.value, commit.value, file.value, config).prettyPrint
                  complete(getFileContents)
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("fileContents", "{\"repo\": \"user/repo\", \"commit\": \"commitHash\", \"file\": \"fileName\"}").getMessage))).prettyPrint)
              }
            } catch{
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          } ~
          path("patch") {
            try {
              val json = queryStr.parseJson.asJsObject()
              (json.fields.get("repo"), json.fields.get("commit"), json.fields.get("file")) match {
                case (Some(repo: JsString), Some(commit: JsString), Some(file: JsString)) =>
                  val getPatch = GithubCommands.getFilePatches(repo.value, commit.value, file.value, config).prettyPrint
                  complete(getPatch)
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("patch", "{\"repo\": \"user/repo\", \"commit\": \"commitHash\", \"file\": \"fileName\"}").getMessage))).prettyPrint)
              }
            } catch{
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          } ~
          path("parent") {
            try {
              val json = queryStr.parseJson.asJsObject()
              (json.fields.get("repo"), json.fields.get("commit"), json.fields.get("file")) match{
                case (Some(repo: JsString), Some(commit: JsString), Some(file: JsString)) =>
                  val getParent = GithubCommands.getFileParents(repo.value, commit.value, file.value, config).prettyPrint
                  complete(getParent)
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("parent", "{\"repo\": \"user/repo\", \"commit\": \"commitHash\", \"file\": \"fileName\"}").getMessage))).prettyPrint)
              }
            } catch {
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          } ~
          path("resetSolr") {
            try {
              val json = queryStr.parseJson.asJsObject()
              json.fields.get("repo") match{
                case Some(JsString("all")) =>
                  val resetSolr = GithubCommands.resetSolrMap("", all=true).prettyPrint
                  complete(resetSolr)
                case Some(JsString(repo)) =>
                  val resetSolr = GithubCommands.resetSolrMap(repo).prettyPrint
                  complete(resetSolr)
                case _ => complete(JsObject(Map("status" -> JsString("error"), "exception" ->
                  JsString(GitServiceException("resetSolr", "{ \"repo\" : (\"user/repo\" or \"all\")").getMessage))).prettyPrint)
              }
            } catch {
              case e: Exception => complete(JsObject(Map("status" -> JsString("error"), "exception" -> JsString(e.getMessage))).prettyPrint)
            }
          }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()
    val route = getRequest(conf)
    val port = if (args.length > 0){
       args(0).toInt
    } else conf.getInt("port")
    if (conf.getBoolean("useSolr")){
      GithubCommands.createSolrMap()
    }
    val bindingFuture = Http().bindAndHandle(route, "localhost", port)
    println(s"Github Service started on Port $port!")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
