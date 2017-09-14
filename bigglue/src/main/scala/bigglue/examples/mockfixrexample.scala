package bigglue.examples

import java.io.File
import java.util.Base64

import scala.sys.process._
import bigglue.computations.Mapper
import bigglue.configurations.{DataStoreBuilder, PipeConfig}
import bigglue.data.serializers.JsonSerializer
import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.exceptions.{UnexpectedPipelineException, UserComputationException}
import bigglue.store.instances.file.{FileSystemDataMap, TextFileDataMap}
import bigglue.store.instances.solr.SolrDataMap
import spray.json._

import scalaj.http.Http

/**
  * Created by chanceroberts on 8/31/17.
  */
object MockProtocol extends DefaultJsonProtocol {
  implicit val gitID: JsonFormat[GitID] = jsonFormat2(GitID)
  implicit val gitRepo: JsonFormat[GitRepo] = jsonFormat2(GitRepo)
  implicit val gitCommitInfo: JsonFormat[GitCommitInfo] = jsonFormat12(GitCommitInfo)
  implicit val gitFeatures: JsonFormat[GitFeatures] = jsonFormat6(GitFeatures)
}

case class GitID(user: String, repo: String) extends Identifiable[GitID]{
  override def mkIdentity(): Identity[GitID] = BasicIdentity(s"$user/$repo")
}

object GitIDSerializer extends JsonSerializer[GitID] {
  import MockProtocol._
  override def serializeToJson_(d: GitID): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): GitID = json.convertTo[GitID]
}

case class GitRepo(gitID: GitID, repoPath: String) extends Identifiable[GitRepo]{
  override def mkIdentity(): Identity[GitRepo] = BasicIdentity(s"${gitID.identity().getId()}:$repoPath")
}

object GitRepoSerializer extends JsonSerializer[GitRepo]{
  import MockProtocol._
  override def serializeToJson_(d: GitRepo): JsObject = {
    JsObject(d.toJson.asJsObject.fields.toList.foldRight(Map[String, JsValue]()){
      case ((key, value), newFields) => value match{
        case JsObject(fields) => newFields ++ fields
        case _ => newFields + (key->value)
      }
    })
  }

  override def deserialize_(json: JsObject): GitRepo = {
    val newJson = JsObject({
      val (a, b) = json.fields.toList.foldRight((Map[String, JsValue](), Map[String, JsValue]())){
        case ((key, jsValue), (newFields, gID)) => key match{
          case "user" | "repo" => (newFields, gID + (key -> jsValue))
          case _ => (newFields + (key -> jsValue), gID)
        }
      }
      a + ("gitID" -> JsObject(b))
    })
    json.convertTo[GitRepo]
  }
}

case class GitCommitInfo(user: String, repo: String, hash: String, repoPath: String,
                        author: String, authorEmail: String, authorDate: String,
                        committer: String, committerEmail: String, title: String, message: String,
                        files: List[String]) extends Identifiable[GitCommitInfo]{ //I need to find a much better way of doing this. :|
  override def mkIdentity(): Identity[GitCommitInfo] = BasicIdentity(s"$user/$repo/$hash:$repoPath")
}

object GitCommitInfoSerializer extends JsonSerializer[GitCommitInfo]{
  import MockProtocol._
  override def serializeToJson_(d: GitCommitInfo): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): GitCommitInfo = json.convertTo[GitCommitInfo]
}

case class GitFeatures(user: String, repo: String, hash: String, repoPath: String, file: String, protobuf: String)
  extends Identifiable[GitFeatures]{
  override def mkIdentity(): Identity[GitFeatures] = BasicIdentity(s"$user/$repo/$hash:$repoPath:$file")
}

object GitFeatureSerializer extends JsonSerializer[GitFeatures]{
  import MockProtocol._
  override def serializeToJson_(d: GitFeatures): JsObject = d.toJson.asJsObject
  override def deserialize_(json: JsObject): GitFeatures = json.convertTo[GitFeatures]
}

case class Clone(repoFolderLocation: String = "mockfixrexample/repos") extends Mapper[GitID, GitRepo](
  input => {
    try {
      val repos = new File(repoFolderLocation)
      if (!repos.exists) repos.mkdir()
      val userRepos = new File(s"$repoFolderLocation/${input.user}")
      if (!userRepos.exists) userRepos.mkdir()
      val repoLocation = s"${input.user}/${input.repo}"
      val trueRepo = new File(s"$repoFolderLocation/$repoLocation")
      if (!(trueRepo.isDirectory && trueRepo.length() > 0)) {
        s"git clone https://github.com/$repoLocation $repoFolderLocation/$repoLocation".!
        List(GitRepo(input, s"$repoFolderLocation/$repoLocation"))
      } else
        List(GitRepo(input, s"$repoFolderLocation/$repoLocation"))
        //throw new UserComputationException("Repo that should be cloned has stuff in it.", None)
    } catch{
      case u: UserComputationException => throw u
      case e: Exception => throw new UserComputationException("File System is not formatted correctly?", Some(e))
    }
  }
)

case class CommitExtraction() extends Mapper[GitRepo, GitCommitInfo](
  input => {
    val lisCommits = s"git -C ${input.repoPath} log --pretty=format:%H".!!
    lisCommits.split("\n").toList.foldRight(List.empty[GitCommitInfo]) {
      case (commit, listOfCommits) =>
        //println(s"Working on commit $commit!")
        val commitInfo = s"git -C ${input.repoPath} show --pretty=fuller --name-only $commit".!!.split("\n")
        //Stupid way of doing this. Fix later!
        val comm = commitInfo(0).substring("commit ".length)
        val checkForMerges = commitInfo(1) match {
          case s if s.substring(0, 7).equals("Merge: ") => 1
          case _ => 0
        }
        val (author, authorEmail) = commitInfo(1 + checkForMerges).substring("Author:     ".length) match {
          case s => s.indexOf('<') match {
            case -1 => (s, "")
            case x => (s.substring(0, x - 1), s.substring(x))
          }
        }
        val authorDate = commitInfo(2 + checkForMerges).substring("AuthorDate: ".length)
        val (blame, blameEmail) = commitInfo(3 + checkForMerges).substring("Commit:     ".length) match {
          case s => s.indexOf('<') match {
            case -1 => (s, "")
            case x => (s.substring(0, x - 1), s.substring(x))
          }
        }
        val commitDate = commitInfo(4 + checkForMerges).substring("CommitDate: ".length)

        def findCommitMessage(message: Array[String], line: Int, currString: String = ""): (String, Int) = if (message.length > line) message(line) match {
          case s if s.length > 4 && s.substring(0, 4).equals("    ") => currString match {
            case "" => findCommitMessage(message, line + 1, message(line).substring(4))
            case _ => findCommitMessage(message, line + 1, s"$currString\n${message(line).substring(4)}")
          }
          case _ => (currString, line)
        } else (currString, line)

        def findFiles(message: Array[String], line: Int, files: List[String] = List()): List[String] = if (message.length > line) message(line) match{
          case s if s.length > 4 && s.substring(0, 4).equals("    ") => findFiles(message, line+2) //Should never happen, but just in case...
          case "" => findFiles(message, line+1, files)
          case x => findFiles(message, line + 1, x :: files)
        } else files

        val (title, nextLine) = findCommitMessage(commitInfo, 6 + checkForMerges)
        val (message, lineAfter) = findCommitMessage(commitInfo, nextLine + 1)
        val gCI = GitCommitInfo(input.gitID.user, input.gitID.repo, comm, input.repoPath, author, authorEmail, authorDate, blame, blameEmail,
          title, message, findFiles(commitInfo, lineAfter))
        gCI :: listOfCommits
    }
  }
)

case class FeatureExtraction() extends Mapper[GitCommitInfo, GitFeatures](
  input => {
    /*var numberOfFailures = 0
    var numberOfSuccesses = 0*/
    val failedMap = new FileSystemDataMap[I[String], I[String]](s"${input.repoPath}/failed")
    val successMap = new FileSystemDataMap[I[String], I[String]](s"${input.repoPath}/success")
    input.files.foldRight(List[GitFeatures]()){
      case (file, list) =>
        val len = file.length
        if (len > 5){
          file.substring(len-5, len) match{
            case ".java" =>
              val f = s"git -C ${input.repoPath} show ${input.hash}:$file".!!
              val fEncoded = Base64.getEncoder.encodeToString(f.getBytes())
              val data = JsObject(Map("name" -> JsString(file), "src" -> JsString(fEncoded)))
              val json = Http("http://52.15.135.195:9002/features/single/json").postData(data.toString).asString.body
              try{
                val map = json.parseJson.asJsObject.fields
                map.get("status") match{
                  case Some(JsString("ok")) => map.get("output") match{
                    case Some(bytes: JsString) =>
                      println(s"File $file had its features extracted!")
                      /*successMap.put(I(s"${input.hash}-success$numberOfSuccesses.java"), I(f))
                      numberOfSuccesses += 1*/
                      val decodedBytes = Base64.getDecoder.decode(bytes.value)
                      GitFeatures(input.user, input.repo, input.hash, input.repoPath, file, new String(decodedBytes)) :: list
                    case _ =>
                      println(f)
                      println(new Exception(s"An error has occurred on file $file!")); list
                  }
                  case Some(e: JsString) if e.value.length() > 5 && e.value.substring(0,5).equals("error") =>
                    map.get("output") match {
                      case Some(exception: JsString) =>
                        println(new Exception(exception.value).getMessage)
                        /*failedMap.put(I(s"${input.hash}-failure$numberOfFailures.java"), I(s"$f\n\n/*\n${new Exception(exception.value).getMessage}\n*/"))
                        numberOfFailures += 1*/
                        list
                      case _ if e.value.length() > 7 => println(new Exception(e.value.substring(6)).getMessage); list
                      case _ => println(new Exception(s"An error has occured on file $file!")); list
                    }
                  case _ => throw new UnexpectedPipelineException(s"Invalid status code on feature extractor!", None)
                }
              }
              catch{
                case e: Exception =>
                  throw new UnexpectedPipelineException(s"Returned value on feature extractor in invalid format.", None)
              }
            case _ => list
          }
        }
        else list
    }
  }
)


object mockfixrexample {
  def IStringToGitID(i: I[String]): GitID = {
    val slash = i.a.indexOf('/')
    GitID(i.a.substring(0, slash), i.a.substring(slash+1))
  }

  def main(args: Array[String]): Unit = {
    val config = PipeConfig.newConfig()
    val textMap = new TextFileDataMap("src/main/mockfixrexample/firstOne.txt")
    val storeBuilder = DataStoreBuilder.load(config)
    val gitID = new SolrDataMap[GitID, GitID](GitIDSerializer, "GitIDs")
    val clonedMap = new SolrDataMap[GitRepo, GitRepo](GitRepoSerializer, "GitRepos")
    val commitInfoMap = new SolrDataMap[GitCommitInfo, GitCommitInfo](GitCommitInfoSerializer, "GitCommitInfo")
    val featureMap = new SolrDataMap[GitFeatures, GitFeatures](GitFeatureSerializer, "GitFeatures")
    import bigglue.pipes.Implicits._
    val pipe = gitID :--Clone()-->clonedMap :--CommitExtraction()-->commitInfoMap :--FeatureExtraction()-->featureMap
    pipe.check(config)
    pipe.init(config)
    textMap.all().foreach{
      input => val gID = IStringToGitID(input)
        gitID.put(gID, gID)
    }
  }
}
