package bigglue.examples

import java.io.File
import java.util.Base64

import scala.sys.process._
import bigglue.computations.{Mapper, Reducer}
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

/**
  * Created by chanceroberts on 4/2/18.
  * This is an example that was created to get a glimpse of BigGlue.
  * In practice, this is kind of simple, being something that clones a repo, takes the commits, and finds which author
  * created the most commits.
  */
object MockProtocol2 extends DefaultJsonProtocol {
  implicit val gitID: JsonFormat[GitID] = jsonFormat2(GitID)
  implicit val gitRepo: JsonFormat[GitRepo] = jsonFormat2(GitRepo)
  implicit val gitCommitInfo: JsonFormat[GitCommitInfo] = jsonFormat3(GitCommitInfo)
  //implicit val gitFeatures: JsonFormat[GitFeatures] = jsonFormat4(GitFeatures)
  implicit val gitCommitGroup: JsonFormat[GitCommitGroups] = jsonFormat2(GitCommitGroups)
}

/**
  * This is an [[Identifiable]] that represents a Github ID for a repo.
  * @param user The user that the repo belongs to.
  * @param repo The repo name.
  */
case class GitID(user: String, repo: String) extends Identifiable[GitID]{
  override def mkIdentity(): Identity[GitID] = BasicIdentity(s"$user/$repo")
}

/**
  * The [[JsonSerializer]] for GitIDs.
  */
object GitIDSerializer extends JsonSerializer[GitID] {
  import MockProtocol2._
  override def serializeToJson_(d: GitID): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): GitID = json.convertTo[GitID]
}

/**
  * An [[Identifiable]] that represents a Git Repo.
  * @param gitID The [[GitID]] for the repo.
  * @param repoPath Where the repo actually lives.
  */
case class GitRepo(gitID: GitID, repoPath: String) extends Identifiable[GitRepo]{
  override def mkIdentity(): Identity[GitRepo] = BasicIdentity(s"${gitID.identity().getId()}:$repoPath")
}

/**
  * A [[JsonSerializer]] for [[GitRepo]]s.
  */
object GitRepoSerializer extends JsonSerializer[GitRepo]{
  import MockProtocol2._
  override def serializeToJson_(d: GitRepo): JsObject = {
    JsObject(flatten(d.toJson.asJsObject.fields))
  }

  def flatten(fields: Map[String, JsValue]): Map[String, JsValue] = {
    fields.toList.foldRight(Map[String, JsValue]()){
      case ((key, value), newFields) => value match{
        case JsObject(moreFields) => newFields ++ moreFields
        case _ => newFields + (key->value)
      }
    }
  }

  override def deserialize_(json: JsObject): GitRepo = {
    val newJson = deflatten(json.fields)
    newJson.convertTo[GitRepo]
  }

  def deflatten(fields: Map[String, JsValue]): JsObject = JsObject({
    val (a, b) = fields.toList.foldRight((Map[String, JsValue](), Map[String, JsValue]())){
      case ((key, jsValue), (newFields, gID)) => key match{
        case "user" | "repo" => (newFields, gID + (key -> jsValue))
        case _ => (newFields + (key -> jsValue), gID)
      }
    }
    a + ("gitID" -> JsObject(b))
  })
}

/**
  * For Solr, we have to make sure that the JSON is completely flat.
  * This is kind of a weird workaround to that, making it so nested repos are merged back and forth.
  */
object NestedWithGitRepo{
  def flatten(jsObj: JsObject): JsObject =
    JsObject(jsObj.fields.toList.foldRight(Map[String, JsValue]()){
      case ((key, value), newFields) => value match{
        case JsObject(fields) if key.equals("gitRepo") => newFields ++ GitRepoSerializer.flatten(fields)
        case JsObject(fields) => newFields ++ fields
        case _ => newFields + (key->value)
      }
    })

  def deflatten(jsObj: JsObject): JsObject =
    JsObject({
      val (a, b) = jsObj.fields.toList.foldRight((Map[String, JsValue](), Map[String, JsValue]())){
        case ((key, jsValue), (newFields, gRepo)) => key match{
          case "user" | "repo" | "repoPath" => (newFields, gRepo + (key -> jsValue))
          case _ => (newFields + (key -> jsValue), gRepo)
        }
      }
      a + ("gitRepo" -> GitRepoSerializer.deflatten(b)) //GitRepoSerializer.deflatten(b).fields
    })
}

/**
  * An [[Identifiable]] for a Commit of a GitRepo, with the hash and the person that created it.
  *
  * @param gitRepo The [[GitRepo]]
  * @param hash The hash of the commit
  * @param author The author that made the commit.
  */
case class GitCommitInfo(gitRepo: GitRepo, hash: String,
                        author: String) extends Identifiable[GitCommitInfo]{ //I need to find a much better way of doing this. :|
  override def mkIdentity(): Identity[GitCommitInfo] =
    BasicIdentity(s"${gitRepo.gitID.user}/${gitRepo.gitID.repo}/$hash:${gitRepo.repoPath}")
}

/**
  * A [[JsonSerializer]] for a [[GitCommitInfo]].
  */
object GitCommitInfoSerializer extends JsonSerializer[GitCommitInfo]{
  import MockProtocol2._
  override def serializeToJson_(d: GitCommitInfo): JsObject = NestedWithGitRepo.flatten(d.toJson.asJsObject)

  override def deserialize_(json: JsObject): GitCommitInfo = {
    val newJson = NestedWithGitRepo.deflatten(json)
    newJson.convertTo[GitCommitInfo]
  }
}


/**
  * The [[Identifiable]] of which author created how many commits.
  * @param author The author that we're tracking.
  * @param gitCommitInfos The amount of git commits the author has made.
  */
case class GitCommitGroups(author: String, gitCommitInfos: Int) extends Identifiable[GitCommitGroups]{
  override def mkIdentity(): Identity[GitCommitGroups] = BasicIdentity(author)
}

/**
  * A [[JsonSerializer]] for [[GitCommitGroups]]
  */
object GitCommitGroupSerializer extends JsonSerializer[GitCommitGroups]{
  import MockProtocol2._

  override def serializeToJson_(d: GitCommitGroups): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): GitCommitGroups = json.convertTo[GitCommitGroups]
}



case class GitFeatures(gitRepo: GitRepo, hash: String, file: String, protobuf: String)
  extends Identifiable[GitFeatures]{
  override def mkIdentity(): Identity[GitFeatures] = BasicIdentity(s"${gitRepo.gitID.user}/${gitRepo.gitID.repo}/$hash:${gitRepo.repoPath}:$file")
}
/*
object GitFeatureSerializer extends JsonSerializer[GitFeatures]{
  import MockProtocol2._
  override def serializeToJson_(d: GitFeatures): JsObject = NestedWithGitRepo.flatten(d.toJson.asJsObject)

  override def deserialize_(json: JsObject): GitFeatures = {
    val newJson = NestedWithGitRepo.deflatten(json)
    newJson.convertTo[GitFeatures]
  }
}
*/

/**
  * This is a simple [[Mapper]] step that clones a [[GitID]] and gives back the [[GitRepo]].
  */
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

/**
  * This is a simple [[Mapper]] step that takes a [[GitRepo]] and gives back the GitCommits and the [[GitCommitInfo]]s that we need.
  */
case class CommitExtraction() extends Mapper[GitRepo, GitCommitInfo](
  input => {
    s"git pull".!
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

        def findCommitMessage(message: Array[String], line: Int, currString: String = ""): (String, Int) = if (message.length > line) message(line) match {
          case s if s.length > 4 && s.substring(0, 4).equals("    ") => currString match {
            case "" => findCommitMessage(message, line + 1, message(line).substring(4))
            case _ => findCommitMessage(message, line + 1, s"$currString\n${message(line).substring(4)}")
          }
          case _ => (currString, line)
        } else (currString, line)

        val (title, nextLine) = findCommitMessage(commitInfo, 6 + checkForMerges)
        val (message, lineAfter) = findCommitMessage(commitInfo, nextLine + 1)
        val gCI = GitCommitInfo(input, comm, author)
        gCI :: listOfCommits
    }
  }
)


/**
  * This is a simple [[Reducer]] step that takes the GitCommitInfo, finds the author, and increments their commit value.
  */
case class FindAuthor() extends Reducer[GitCommitInfo, GitCommitGroups](
  i => BasicIdentity(i.author),
  i => o => {
    println(o.gitCommitInfos); GitCommitGroups(i.author, o.gitCommitInfos+1)
  },
  GitCommitGroups("", 0)
)

/*
case class FeatureExtraction() extends Mapper[GitCommitInfo, GitFeatures](
  input => {
    /*var numberOfFailures = 0
    var numberOfSuccesses = 0
    val failedMap = new FileSystemDataMap[I[String], I[String]](s"${input.gitRepo.repoPath}/failed")
    val successMap = new FileSystemDataMap[I[String], I[String]](s"${input.gitRepo.repoPath}/success")*/
    input.files.foldRight(List[GitFeatures]()){
      case (file, list) =>
        val len = file.length
        if (len > 5){
          file.substring(len-5, len) match{
            case ".java" =>
              val f = s"git -C ${input.gitRepo.repoPath} show ${input.hash}:$file".!!
              val fEncoded = Base64.getEncoder.encodeToString(f.getBytes())
              val data = JsObject(Map("name" -> JsString(file), "src" -> JsString(fEncoded)))
              println(data)
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
                      GitFeatures(input.gitRepo, input.hash, file, new String(decodedBytes)) :: list
                    case _ =>
                      println(f)
                      println(new Exception(s"An error has occurred on file $file!")); list
                  }
                  case Some(e: JsString) if e.value.length() > 5 && e.value.substring(0,5).equals("error") =>
                    map.get("output") match {
                      case Some(exception: JsString) =>
                        //println(map)
                        println(new Exception(exception.value).getMessage)
                        /*failedMap.put(I(s"${input.hash}-failure$numberOfFailures.java"), I(s"${new String(Base64.getDecoder.decode(fEncoded))}\n\n/*\n$data\n${new Exception(exception.value).getMessage}\n*/"))
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
*/


/**
  * This is the actual code for the simple example.
  * It first gets the configuration file with [[PipeConfig.newConfig]]
  * Then, it creates a few [[SolrDataMap]]s to put the data in within each step.
  * To start, it also puts in a Git ID into gitID for the sake of having a starting point for the example.
  * Then, with gitID:--Clone()-->clonedMap:--CommitExtraction()-->commitInfoMap:-+FindAuthor()+->authorMap, it creates this:
  * [[bigglue.pipes.ReducerPipe]]([[bigglue.pipes.MapperPipe]]([[bigglue.pipes.MapperPipe]]([[bigglue.pipes.Implicits.DataNode]](gitID), Clone(), [[bigglue.pipes.Implicits.DataNode]](clonedMap)), CommitExtraction(), [[bigglue.pipes.Implicits.DataNode]](commitInfoMap)), FindAuthor(), [[bigglue.pipes.Implicits.DataNode]](authorMap))
  * Then, with a pipe, we run [[bigglue.pipes.Pipe.check]] and [[bigglue.pipes.Pipe.init]] to initialize the pipeline.
  * Finally, we run [[bigglue.pipes.Pipe.persist]] to start/resume the pipeline.
  */
object mockfixrexample {
  /**
    * This is literally just a serialization thing done here just so we have some sort of data to work with.
    * @param i The string of the User and Github Repo
    * @return A [[GitID]] from i.
    */
  def IStringToGitID(i: I[String]): GitID = {
    val slash = i.a.indexOf('/')
    GitID(i.a.substring(0, slash), i.a.substring(slash+1))
  }

  /** Configuration File in resources/application.conf */
  def main(args: Array[String]): Unit = {
    val config = PipeConfig.newConfig()
    val textMap = new TextFileDataMap("src/main/mockfixrexample/firstOne.txt")
    val gitID = new SolrDataMap[GitID, GitID](GitIDSerializer, "GitIDs")
    textMap.all().foreach{
      input => val gID = IStringToGitID(input)
        gitID.put_(gID, gID)
    }
    val clonedMap = new SolrDataMap[GitRepo, GitRepo](GitRepoSerializer, "GitRepos")
    val commitInfoMap = new SolrDataMap[GitCommitInfo, GitCommitInfo](GitCommitInfoSerializer, "GitCommitInfo")
    val authorMap = new SolrDataMap[Identity[GitCommitGroups], GitCommitGroups](GitCommitGroupSerializer, "GitAuthors")
    //val featureMap = new SolrDataMap[GitFeatures, GitFeatures](GitFeatureSerializer, "GitFeatures")
    import bigglue.pipes.Implicits._
    val pipe = gitID:--Clone()-->clonedMap:--CommitExtraction()-->commitInfoMap:-+FindAuthor()+->authorMap //:--FeatureExtraction()-->featureMap
    pipe.check(config)
    pipe.init(config)
    pipe.persist()
  }
}
