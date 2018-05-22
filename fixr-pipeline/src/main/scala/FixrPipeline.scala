import java.util.Base64

import bigglue.computations.Mapper
import bigglue.configurations.PipeConfig
import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.data.serializers.JsonSerializer
import bigglue.store.DataMap
import bigglue.store.instances.InMemDataMap
import bigglue.store.instances.file.{FileSystemDataMap, TextFileDataMap}
import bigglue.store.instances.solr.SolrDataMap
import spray.json._

import scalaj.http.{Http, HttpOptions}
import scala.sys.process._

/**
  * Created by chanceroberts on 11/1/17.
  */
object FixrProtocol extends DefaultJsonProtocol {
  implicit val gitCommit: JsonFormat[GitCommit] = jsonFormat2(GitCommit)
  implicit val gitFiles: JsonFormat[GitFiles] = jsonFormat3(GitFiles)
}

object Poster{
  def verify(posted: Map[String, JsValue]): Boolean = posted.get("status") match{
    case Some(JsString("ok")) => true
    case _ => false
  }

  def post(url: String, postData: JsObject): Map[String, JsValue] = {
    Http(url).timeout(1000, 10000).postData(postData.prettyPrint)
      .header("Content-Type", "application/json").option(HttpOptions.followRedirects(true))
      .asString.body.parseJson.asJsObject.fields
  }

  def postMap(url: String, postData: JsObject): Map[String, JsValue] = {
    val postTest = post(url, postData)
    if (!verify(postTest)) throw new Exception(s"Service $url did not work!")
    postTest
  }

  def postList(url: String, postData: JsObject, retVal: String = "results"): List[JsValue] ={
    val postTest = post(url, postData)
    (postTest.get("status"), postTest.get(retVal)) match{
      case (Some(JsString("ok")), Some(j: JsArray)) => j.elements.toList
      case (Some(JsString("ok")), _) => List()
      case (Some(JsString("error")), _) => throw new Exception(s"Service $url threw error: ${postTest("exception")}")
      case (_, _) => throw new Exception(s"Service $url did not work!")
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

case class GitFeatureExtracted(gitFiles: GitFiles, features: String) extends Identifiable[GitFeatureExtracted]{
  override def mkIdentity(): Identity[GitFeatureExtracted] = BasicIdentity(gitFiles.identity().getId())
}

object GitCommitSerializer extends JsonSerializer[GitCommit]{
  import FixrProtocol._
  override def deserialize_(json: JsObject): GitCommit = json.convertTo[GitCommit]

  override def serializeToJson_(d: GitCommit): JsObject = d.toJson.asJsObject
}

object GitCommitInfoSerializer extends JsonSerializer[GitCommitInfo]{
  override def deserialize_(json: JsObject): GitCommitInfo = {
    def extractStrFromJsValue(jsValue: JsValue): String = jsValue match{
      case JsString(s) => s
      case JsArray(Vector(JsString(s))) => s
    }
    val (gitRepo, hash, info) = json.fields.foldRight(("", "", Map[String, String]())){
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

object GitFileSerializer extends JsonSerializer[GitFiles]{
  import FixrProtocol._
  override def deserialize_(json: JsObject): GitFiles = json.convertTo[GitFiles]

  override def serializeToJson_(d: GitFiles): JsObject = d.toJson.asJsObject
}

object GitFeatureExtractedSerializer extends JsonSerializer[GitFeatureExtracted]{
  override def serializeToJson_(d: GitFeatureExtracted): JsObject = {
    val jsonStart = GitFileSerializer.serializeToJson_(d.gitFiles)
    JsObject(jsonStart.fields + ("features" -> JsString(d.features)))
  }

  override def deserialize_(json: JsObject): GitFeatureExtracted = {
    val (features, newJsObject) = json.fields.foldRight(("", Map[String, JsValue]())){
      case (("features", JsString(feature)), (_, map)) => (feature, map)
      case ((key, value), (feature, map)) => (feature, map + (key->value))
    }
    GitFeatureExtracted(GitFileSerializer.deserialize_(JsObject(newJsObject)), features)
  }
}

case class GitCloneAndGetCommits() extends Mapper[I[String], GitCommit] (
  input => {
    val files = new FileSystemDataMap[I[String], I[String]]("repos")
    val splitter = input.i.indexOf("/") match{
      case -1 => throw new Exception(s"Invalid GitID ${input.i}!")
      case x => x
    }
    files.get(I(s"${input.i}/")) match{
      case None => files.put(I(s"repos/${input.i}/"))
      case Some(_) => ()
    }
    files.get(I(s"${input.a}")) match{
      case Some(I("")) => s"git -C repos/${input.a.substring(0, input.a.indexOf('/'))} clone https://github.com/${input.a}".!
      case _ => s"git -C repos/${input.a} pull".!
    }
    val list = s"git -C repos/${input.a} log --all --pretty=format:%H -- *.java".!!.split("\n").toList

    list.foldRight(List[GitCommit]()){
      case (hash, lis) => GitCommit({input.a}, hash) :: lis
    }
    /*println(Poster.postList("http://localhost:8080/clone", JsObject(Map("repo" -> JsString(input.i)))))
    Poster.postList("http://localhost:8080/getCommits", JsObject(Map("repo" -> JsString(input.i)))).foldRight(List[GitCommit]()){
      case (JsString(hash), listOfCommits) => GitCommit(input.i, hash) :: listOfCommits
      case (_, listOfCommits) => listOfCommits
    }*/
  }
)

case class GitCommitInformation() extends Mapper[GitCommit, GitCommitInfo] (
  input => {
    //18.220.127.2
    val thisMap = Poster.post("http://localhost:8080/commitInformation",
      JsObject(Map("repo" -> JsString(input.gitRepo), "commit" -> JsString(input.hash))))
    val newMap = thisMap.foldRight(Map[String, String]()){
      case (("status", _), same) => same
      case ((key, JsString(value)), mapOfFields) => mapOfFields + (key -> value)
      case ((key, JsNumber(num)), mapOfFields) => mapOfFields + (key -> num.toString)
    }
    List(GitCommitInfo(input.gitRepo, input.hash, newMap))
  }
)

case class GitFilesFromCommits() extends Mapper[GitCommit, GitFiles](
  input => {
    println(s"Starting reading from ${input.gitRepo}:${input.hash}!")
    val listOfFiles = s"git -C repos/${input.gitRepo} show --oneline --name-only ${input.hash} -- *.java".!!
    listOfFiles.split("\n").tail.foldRight(List[GitFiles]()){
      case (str, lis)  if !str.equals("") => println(str); GitFiles(input.gitRepo, input.hash, str) :: lis
      case (_, lis) => lis
    }
    /*Poster.postList("http://localhost:8080/getFiles",
      JsObject(Map("repo" -> JsString(input.gitRepo), "commit" -> JsString(input.hash), "pattern" -> JsString("*.java"))))
      .map{case JsString(file) => println(file); GitFiles(input.gitRepo, input.hash, file)}*/
  }
)

case class GitFeatures(failureMap: TextFileDataMap) extends Mapper[GitFiles, GitFeatureExtracted](
  input => {
    println(s"Working on: ${input.gitRepo}:${input.hash}/${input.file}")
    val fileContents = Poster.postList("http://localhost:8080/fileContents",
      JsObject(Map("repo" -> JsString(input.gitRepo), "commit" -> JsString(input.hash), "file" -> JsString(input.file)))).foldRight("") {
      case (JsString(j), str) => s"$j\n$str"
      case (_, str) => str
    }
    val fEncoded = Base64.getEncoder.encodeToString(fileContents.getBytes())
    val results = Poster.post("http://52.15.135.195:9002/features/single/json", JsObject(Map("name"->JsString(input.file),
      "src"->JsString(fileContents))))
    if (!Poster.verify(results)) results.get("output") match{
      case Some(JsString(exception)) =>
        failureMap.put(I(s"The feature extraction failed with exception $exception on file ${input.getId()}"))
        throw new Exception(s"The feature extractor failed with exception $exception!")
      case _ =>
        failureMap.put(I(s"The feature extraction failed on file ${input.getId()}!"))
        throw new Exception("The feature extractor failed!")
    }
    val decoded = results.get("output") match{
      case Some(JsString(j)) => Base64.getDecoder.decode(j)
      case _ =>
        failureMap.put(I(s"An error occurred on the file ${input.getId()}"))
        throw new Exception(s"An error occurred on the file ${input.getId()}")
    }
    List(GitFeatureExtracted(input, new String(decoded)))
  }
)



object FixrPipeline{
  def main(args: Array[String]): Unit = {
    val config = PipeConfig.newConfig()
    val fEErrorLog = new TextFileDataMap("src/main/fixr-pipeline/feature-extraction-error.log")
    val textMap = new TextFileDataMap("src/main/fixr-pipeline/firstOne.txt")
    val gitToClone = new InMemDataMap[I[Int], I[String]]
    val gitCommit = new SolrDataMap[GitCommit, GitCommit](GitCommitSerializer, "fP-commits")
    val gitCommitInfo = new SolrDataMap[GitCommitInfo, GitCommitInfo](GitCommitInfoSerializer, "fP-commitInfo")
    val gitFiles = new SolrDataMap[GitFiles, GitFiles](GitFileSerializer, "fP-gitFiles")
    val featureMap = new SolrDataMap[GitFeatureExtracted, GitFeatureExtracted](GitFeatureExtractedSerializer, "fP-featureExtr")
    import bigglue.pipes.Implicits._
    val fixrPipe = gitToClone:--GitCloneAndGetCommits()-->gitCommit:--GitFilesFromCommits()-->
      gitFiles
    fixrPipe.check(config)
    fixrPipe.init(config)
    fixrPipe.persist()
    textMap.all().foldLeft(0){
      case (num, input) =>
        gitToClone.put(I(num), input)
        num + 1
    }
  }

}