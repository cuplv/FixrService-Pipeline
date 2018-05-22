import java.util.Base64

import bigglue.computations.Mapper
import bigglue.configurations.PipeConfig
import bigglue.data.serializers.JsonSerializer
import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.store.DataMap
import bigglue.store.instances.InMemDataMap
import bigglue.store.instances.file.{FileSystemDataMap, TextFileDataMap}
import bigglue.store.instances.solr.SolrDataMap
import spray.json._

import scala.sys.process._

/**
  * Created by chanceroberts on 12/14/17.
  */

case class GitID(user: String, repo: String, lastChecked: Option[String]) extends Identifiable[GitID]{
  override def mkIdentity(): Identity[GitID] = BasicIdentity(s"$user/$repo")
}

case class GitComm(gitID: String, hash: String) extends Identifiable[GitComm]{
  override def mkIdentity(): Identity[GitComm] = BasicIdentity(s"$gitID/$hash")
}

case class GitFile(gitID: String, hash: String, name: String, contents: Option[String] = None) extends Identifiable[GitFile]{
  override def mkIdentity(): Identity[GitFile] = BasicIdentity(s"$gitID/$hash/$name")
}

/*case class GitFeatures(gitID: String, hash: String, name: String, features: String)
  extends Identifiable[GitFeatures]{
  override def mkIdentity(): Identity[GitFeatures] = BasicIdentity(s"$gitID/$hash/$name")
}*/

object MockProtocol extends DefaultJsonProtocol{
  implicit val gID: JsonFormat[GitID] = jsonFormat3(GitID)
  implicit val gCom: JsonFormat[GitComm] = jsonFormat2(GitComm)
  implicit val gFil: JsonFormat[GitFile] = jsonFormat4(GitFile)
}

case class GIDSerializer() extends JsonSerializer[GitID]{
  import MockProtocol.gID
  override def serializeToJson_(d: GitID): JsObject = d.toJson.asJsObject
  override def deserialize_(json: JsObject): GitID = json.convertTo[GitID]
}

case class GCSerializer() extends JsonSerializer[GitComm]{
  import MockProtocol.gCom
  override def serializeToJson_(d: GitComm): JsObject = d.toJson.asJsObject
  override def deserialize_(json: JsObject): GitComm = json.convertTo[GitComm]
}

case class GFSerializer() extends JsonSerializer[GitFile]{
  import MockProtocol.gFil
  override def serializeToJson_(d: GitFile): JsObject = d.toJson.asJsObject
  override def deserialize_(json: JsObject): GitFile = json.convertTo[GitFile]
}

case class CreateDir() extends Mapper[I[String], GitID](input => {
  val files = new FileSystemDataMap[I[String], I[String]]("repos")
  val splitter = input.i.indexOf("/") match{
    case -1 => throw new Exception(s"Invalid GitID ${input.i}!")
    case x => x
  }
  files.get(I(s"${input.i}/")) match{
    case None => files.put(I(s"repos/${input.i}/"))
    case Some(_) => ()
  }
  List(GitID(input.i.substring(0, splitter), input.i.substring(splitter+1), None))
})

case class Clone() extends Mapper[GitID, GitID](input => {
  val files = new FileSystemDataMap[I[String], I[String]]("repos")
  files.get(I(s"${input.user}/${input.repo}")) match{
    case Some(I("")) => s"git -C repos/${input.user} clone https://github.com/${input.user}/${input.repo}".!
    case _ => s"git -C repos/${input.user}/${input.repo} pull".!
  }
  List(input)
})

case class Pull() extends Mapper[GitID, GitID](input => {
  val files = new FileSystemDataMap[I[String], I[String]]("repos")
  files.get(I(s"${input.user}/${input.repo}")) match{
    case Some(I("")) => throw new Exception(s"Git Repository $input has not been cloned.")
    case _ => s"git -C repos/${input.user}/${input.repo} pull".!
  }
  List(input)
})

case class CheckCommits() extends Mapper[GitID, I[(GitID, GitComm)]](input => {
  val list = (input.lastChecked match{
    case None => s"git -C repos/${input.user}/${input.repo} log --all --pretty=format:%H -- *.java".!!
    case Some(x) => s"git -C repos/${input.user}/${input.repo} log --since $x --pretty=format:%H -- *.java".!!
  }).split("\n").toList
  val date: Option[String] = if (list.nonEmpty){
    val dte = s"git -C repos/${input.user}/${input.repo} show --date=unix --format=%ad -s ${list.head}".!!
    Some((dte.split("\n")(0).toInt+1).toString)
  } else None
  list.foldRight(List[I[(GitID, GitComm)]]()){
    case (hash, lis) => I((GitID(input.user, input.repo, date), GitComm(s"${input.user}/${input.repo}", hash))) :: lis
  }
})

case class CreateFiles() extends Mapper[GitComm, GitFile](input => {
  val listOfFiles = s"git -C repos/${input.gitID} show --oneline --name-only ${input.hash} -- *.java".!!
  listOfFiles.split("\n").foldRight(List[GitFile]()){
    case (str, lis)  if !str.equals("") => GitFile(input.gitID, input.hash, str) :: lis
    case (_, lis) => lis
  }
})

case class FeatureExtraction() extends Mapper[GitFile, GitFile](input => {
  val fileContents = s"git -C repos/${input.gitID} show ${input.hash}:${input.name}".!!
  val fEncoded = Base64.getEncoder.encodeToString(fileContents.getBytes())
  val results = Poster.post("http://52.15.135.195:9002/features/single/json", JsObject(Map("name"->JsString(input.name),
    "src"->JsString(fEncoded))))
  if (!Poster.verify(results)) results.get("output") match{
    case Some(JsString(exception)) =>
      throw new Exception(s"The feature extractor failed with exception $exception!")
    case _ =>
      throw new Exception("The feature extractor failed!")
  }
  val decoded = results.get("output") match{
    case Some(JsString(j)) => Base64.getDecoder.decode(j)
    case _ =>
      throw new Exception(s"An error occurred on the file ${input.getId()}")
  }
  List(GitFile(input.gitID, input.name, input.hash, Some(new String(decoded))))
})

object ExampleFromFixr {
  def main(args: Array[String]): Unit = {
    import bigglue.pipes.Implicits._
    val config = PipeConfig.newConfig()
    val toClone = new TextFileDataMap("src/main/fixr-pipeline/firstOne.txt")
    val gitToClone = new InMemDataMap[I[Int], I[String]]
    val clone = new SolrDataMap[GitID, GitID](GIDSerializer(), "toClone")
    val cloned = new SolrDataMap[GitID, GitID](GIDSerializer(), "cloned")
    val commits = new SolrDataMap[GitComm, GitComm](GCSerializer(), "commits")
    val pulled = new SolrDataMap[GitID, GitID](GIDSerializer(), "toPull")
    val files = new SolrDataMap[GitFile, GitFile](GFSerializer(), "file")
    val extrFeat = new SolrDataMap[GitFile, GitFile](GFSerializer(), "fileExtracted")
    val featExtrPipe = commits:--CreateFiles()-->files:--FeatureExtraction()-->extrFeat
    val createCommsPipe = cloned:--CheckCommits()-->(DataNode(pulled), featExtrPipe)
    val pipe = gitToClone:--CreateDir()-->clone:--Clone()-->createCommsPipe
    //:--Pull()-->cloned
    pipe.check(config)
    pipe.init(config)
    pipe.persist()
    /*toClone.all.foldLeft(0){
      case (num, str) =>
        gitToClone.put(I(num), str)
        num+1
    }*/
  }
}
