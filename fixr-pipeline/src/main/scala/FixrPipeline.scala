import java.util.Base64

import bigglue.computations.Mapper
import bigglue.configurations.PipeConfig
import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.data.serializers.JsonSerializer
import spray.json._

import scalaj.http.{Http, HttpOptions}

/**
  * Created by chanceroberts on 11/1/17.
  */
object Poster{
  def post(url: String, postData: JsObject): Map[String, JsValue] = {
    Http(url).timeout(1000, 3600000).postData(postData.prettyPrint)
      .header("Content-Type", "application/json").option(HttpOptions.followRedirects(true))
      .asString.body.toJson.asJsObject.fields
  }

  def postMap(url: String, postData: JsObject): Map[String, JsValue] = {
    val postTest = post(url, postData)
    postTest.get("status") match{
      case Some(JsString("ok")) => postTest
      case _ => throw new Exception("Github Service did not work!")
    }
  }
  def postList(url: String, postData: JsObject, retVal: String = "results"): List[JsValue] ={
    val postTest = post(url, postData)
    (postTest.get("status"), postTest.get(retVal)) match{
      case (Some(JsString("ok")), Some(j: JsArray)) => j.elements.toList
      case (Some(JsString("ok")), _) => List()
      case (Some(JsString("error")), _) => throw new Exception(s"Github Service threw error: ${postTest("exception")}")
      case (_, _) => throw new Exception("Github Service did not work!")
    }
  }
}

case class GitCommit(gitRepo: String, hash: String) extends Identifiable[GitCommit]{
  override def mkIdentity(): Identity[GitCommit] = BasicIdentity(s"$gitRepo:$hash")
}

case class GitCommitInfo(gitRepo: String, hash: String, information: Map[String, String])
  extends Identifiable[GitCommitInfo]{
  override def mkIdentity(): Identity[GitCommitInfo] = BasicIdentity(s"$gitRepo:$hash")
}

case class GitFiles(gitRepo: String, hash: String, file: String) extends Identifiable[GitFiles]{
  override def mkIdentity(): Identity[GitFiles] = BasicIdentity(s"$gitRepo:$hash/$file")
}

case class GitFeatureExtracted(gitFiles: GitFiles, features: String) extends Identifiable[GitFiles]{
  override def mkIdentity(): Identity[GitFiles] = gitFiles.identity()
}

object GitCommitInfoSerializer extends JsonSerializer[GitCommitInfo]{
  override def deserialize_(json: JsObject): GitCommitInfo = {
    def extractStrFromJsValue(jsValue: JsValue): String = jsValue match{
      case JsString(s) => s
      case JsArray(Vector(JsString(s))) => s
    }
    val (gitRepo, hash, info) = json.fields.foldRight(("", "", Map()[String, String])){
      case (("gitRepo", jsval), (_, hsh, inf)) => (extractStrFromJsValue(jsval), hsh, inf)
      case (("hash", jsval), (gRepo, _, inf)) => (gRepo, extractStrFromJsValue(jsval), inf)
      case ((str, jsval), (gRepo, hsh, inf)) => (gRepo, hsh, inf+(str->extractStrFromJsValue(jsval)))
    }
    GitCommitInfo(gitRepo, hash, info)
  }

  override def serializeToJson_(d: GitCommitInfo): JsObject = {
    val startingFields: Map[String, JsValue] = Map("gitRepo" -> JsString(d.gitRepo), "hash" -> JsString(d.hash))
    val fields = d.information.foldRight(startingFields){
      case ((key, value), oldFields) => oldFields + (key->JsString(value))
    }
    JsObject(fields)
  }
}

case class GitCloneAndGetCommits() extends Mapper[I[String], GitCommit] (
  input => {
    Poster.postList("http://18.220.127.2:8080/clone", JsObject(Map("repo" -> JsString(input.i))))
    Poster.postList("http://18.220.127.2:8080/getCommits", JsObject(Map("repo" -> JsString(input.i)))).foldRight(List[GitCommit]()){
      case (JsString(hash), listOfCommits) => GitCommit(input.i, hash) :: listOfCommits
      case (_, listOfCommits) => listOfCommits
    }
  }
)

case class GitCommitInformation() extends Mapper[GitCommit, GitCommitInfo] (
  input => {
    val thisMap = Poster.post("http://18.220.127.2:8080/commitInformation",
      JsObject(Map("repo" -> JsString(input.gitRepo), "commit" -> JsString(input.hash))))
    thisMap.foldRight(Map[String, String]()){
      case (("status", _), same) => same
      case ((key, JsString(value)), mapOfFields) => mapOfFields + (key -> value)
      case ((key, JsNumber(num)), mapOfFields) => mapOfFields + (key -> num.toString)
    }
    ???
  }
)

case class GitFilesFromCommits() extends Mapper[GitCommit, GitFiles](
  input => {
    Poster.postList("http://18.220.127.2:8000/getFiles",
      JsObject(Map("repo" -> JsString(input.gitRepo), "commit" -> JsString(input.hash), "pattern" -> JsString("*.java"))))
      .map{case JsString(file) => GitFiles(input.gitRepo, input.hash, file)}
  }
)

case class GitFeatures() extends Mapper[GitFiles, GitFeatureExtracted](
  input => {
    val fileContents = Poster.postList("http://18.220.127.2:8000/fileContents",
      JsObject(Map("repo" -> JsString(input.gitRepo), "commit" -> JsString(input.hash), "file" -> JsString(input.file)))).foldRight("") {
      case (JsString(j), str) => s"$j\n$str"
      case (_, str) => str
    }
    val fEncoded = Base64.getEncoder.encodeToString(fileContents.getBytes())

    ???
  }
)



object FixrPipeline{
  def main(args: Array[String]): Unit = {
    val config = PipeConfig.newConfig()

  }
}