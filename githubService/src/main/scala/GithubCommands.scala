import java.io.File

import bigglue.data.{BasicIdentity, Identifiable, Identity}
import bigglue.data.serializers.JsonSerializer
import bigglue.store.instances.solr.SolrDataMap
import com.typesafe.config.Config
import spray.json._

import scala.sys.process._

/**
  * Created by chanceroberts on 9/26/17.
  */

case class GitRepo(name: String, lastCommit: Option[String]) extends Identifiable[GitRepo]{
  override def mkIdentity(): Identity[GitRepo] = BasicIdentity(name)
}

object GitRepoSer extends DefaultJsonProtocol{
  implicit val gRepo: JsonFormat[GitRepo] = jsonFormat2(GitRepo)
}

object GitRepoSerializer extends JsonSerializer[GitRepo] {
  import GitRepoSer._
  override def serializeToJson_(d: GitRepo): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): GitRepo = json.convertTo[GitRepo]
}

object GithubCommands {
  val solrMap = new SolrDataMap[String, GitRepo](GitRepoSerializer, "GitRepos")
  private def hasBeenCloned(repo: String): Boolean ={
    solrMap.get(repo) match{
      case Some(gitRepo) => true
      case _ => false
    }
  }

  def clone(path: String, config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    val repos = new File(startingDirectory)
    if (!repos.exists()) repos.mkdir()
    val user = path.indexOf('/') match{
      case -1 => return JsObject(Map("status" -> JsString("error"),
        "exception" -> new JsStringGitException(CloneException(path, "Repo does not exist."))))
      case 0 => return JsObject(Map("status" -> JsString("error"),
        "exception" -> new JsStringGitException(CloneException(path, "User does not exist."))))
      case x => path.substring(0, x)
    }
    // TODO: Make this section more BigGlue-like. Reformat the File System Abstraction to be able to create folders like this.
    val repo = path.substring(path.indexOf('/')+1)
    val userDir = new File(s"$startingDirectory/$user/")
    if (!userDir.exists()) userDir.mkdir()
    val repoDir = new File(s"$startingDirectory/$user/$repo")
    if (!repoDir.exists()) repoDir.mkdir()
    repoDir.listFiles().length match{
      case 0 => s"git clone https://github.com/$path $startingDirectory/$user/$repo".!
      case _ => // Assume that it's already cloned.
        try {
          s"git -C $startingDirectory/$user/$repo pull".!
        } catch{
          case e: Exception => // The below should NEVER occur.
            return JsObject("status" -> JsString("error"),
            "exception" -> new JsStringGitException(CloneException(path, s"The path $user/$repo is not a Git Repo.")))
        }
    }
    solrMap.put(path, GitRepo(path, None))
    JsObject(Map("status" -> JsString("ok")))
  }

  def pull(path: String, config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    if (hasBeenCloned(path)){
        try{
          s"git -C $startingDirectory/$path pull".!
          JsObject(Map("status" -> JsString("ok")))
        } catch {
          case e: Exception => JsObject("status" -> JsString("error"),
            "exception" -> new JsStringGitException(PullException(path, e.getMessage)))
        }
    } else{
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(PullException(path, "Repo does not exist.")))
    }
  }

  def getCommits(path: String, config: Config, sinceLastTime: Boolean = false, pattern: Option[String] = None): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    if (hasBeenCloned(path)){
      val startingLog = s"git -C $startingDirectory/$path log"
      val sinceLog = if (sinceLastTime){
        val gitRepo = solrMap.get(path).get
        gitRepo.lastCommit match{
          case Some(s) => s"$startingLog $s"
          case _ => startingLog
        }
      } else startingLog
      val commitHashes = pattern match{
        case None => s"$sinceLog --pretty=format:%H".!!
        case Some(pat) => s"$sinceLog --pretty=format:%H -- $pat".!!
      }
      // TODO: Store last commit in repo information
      val commitArray = commitHashes.split("\n")
      if (commitArray.nonEmpty)
        solrMap.put(path, GitRepo(path, Some(commitArray(0))))
      JsObject("status" -> JsString("ok"),
        "results" -> JsArray(commitHashes.split("\n").toVector.map(JsString(_))))
    } else{
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(CommitsException(path, "Repo does not exist.")))
    }
  }

  def extractCommit(repo: String, hash: String, config: Config): JsObject = {
    def addNextToMap(message: String, extractionList: List[String], map: Map[String, JsString] = Map()): Map[String, JsString] = extractionList match {
      case Nil => map
      case head :: rest =>
        val splitVal = message.indexOf(0) // Time for some really weird errors. :|
        addNextToMap(message.substring(splitVal+1), rest, map + (message -> JsString(message.substring(0, splitVal))))
    }
    val startingDirectory = config.getString("repoLocation")
    if (hasBeenCloned(repo)){
      val commitExtract =  s"git -C $startingDirectory/$repo show --date=unix --format=%an%x00%ae%00%cn%00%ce%x00%s%x00%b%x00%ad $hash".!!
      val extractionList = List("name", "email", "commitName", "commitEmail", "subject", "body", "date")
      JsObject("status" -> JsString("ok"), "results" -> JsObject(addNextToMap(commitExtract, extractionList)))
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(CommitInfoException(repo, hash, "Repo does not exist.")))
    }
  }

  def getFiles(repo: String, hash: String, pattern: Option[String], config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    if (hasBeenCloned(repo)){
      val startingShow = s"git -C $startingDirectory/$repo show --oneline --name-only $hash"
      val listOfFiles = pattern match{
        case None => s"$startingShow".!!
        case Some(pat) => s"$startingShow -- $pat".!!
      }
      JsObject("status" -> JsString("ok"), "results" -> JsArray(listOfFiles.split("\n").toVector.map(JsString(_))))
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(CommitGetFileException(repo, hash, "Repo does not exist.")))
    }
  }

  def getFileContents(repo: String, hash: String, fileName: String, config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    if (hasBeenCloned(repo)) {
      val fileContents = s"git -C $startingDirectory/$repo show $hash:$fileName".!!
      fileContents.length() match{
        case 0 => JsObject("status" -> JsString("ok"), "results" -> JsString(""), "empty" -> JsTrue)
        case _ => JsObject("status" -> JsString("ok"), "results" -> JsString(fileContents), "empty" -> JsFalse)
      }
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(FileGetContentsException(repo, hash, fileName, "Repo does not exist.")))
    }
  }

  def getFilePatches(repo: String, hash: String, fileName: String, config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    if (hasBeenCloned(repo)){
      //As for right now, I'm leaving it as a JsString. I'm still slightly unsure on how patches work in git... :|
      JsObject("status" -> JsString("ok"), "results" -> JsString(s"git -C $startingDirectory/$repo show $hash -- $fileName".!!))
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(FilePatchException(repo, hash, fileName, "Repo does not exist.")))
    }
  }

  def getFileParents(repo: String, hash: String, fileName: String, config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    if (hasBeenCloned(repo)) {
      //As for right now, I'm leaving it as a JsString. I'm unsure how Ken actually uses this... :|
      JsObject("status" -> JsString("ok"),
        "results" -> JsString(s"git -C $startingDirectory/$repo log --full-history --parents $hash -- $fileName".!!))
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(FileParentException(repo, hash, fileName, "Repo does not exist.")))
    }
  }
}
