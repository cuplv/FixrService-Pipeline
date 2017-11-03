import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.data.serializers.JsonSerializer
import bigglue.store.instances.file.FileSystemDataMap
import bigglue.store.instances.solr.SolrDataMap
import com.typesafe.config.{Config, ConfigFactory}
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

  override def deserialize_(json: JsObject): GitRepo = {
    JsObject(json.fields.foldRight(Map[String, JsValue]()) {
      case ((str, value), newFields) => value match { // Yay, schemaless stuff! :\
        case JsArray(Vector(JsNumber(n))) => newFields + (str -> JsString(n.toString))
        case JsArray(Vector(j: JsValue)) => newFields + (str -> j)
        case JsNumber(n) => newFields + (str -> JsString(n.toString))
        case _ => newFields + (str -> value)
      }
    }).convertTo[GitRepo]
  }

}

object GithubCommands {
  val solrMapOpt: Option[SolrDataMap[String, GitRepo]] = try{
    Some(new SolrDataMap[String, GitRepo](GitRepoSerializer, "gitservice_GitRepos"))
  } catch {
    case e: Exception => None
  }
  val startingDirectory: String = ConfigFactory.load().getString("repoLocation")
  val fileSystemMap = new FileSystemDataMap[I[String], I[String]](startingDirectory)

  private def hasBeenCloned(path: String): Boolean = {
    fileSystemMap.get(I(path)) match{
      case None | Some(I("")) => false
      case _ => true
    }
  }

  private def repoToPath(repo: String): String = repo.indexOf("/") match{
    case -1 => throw new Exception("Malformed Repo: The Repo does not exist. (No / found anywhere)")
    case 0 => throw new Exception("Malformed Repo: The User does not exist. (Found as \"/repo\")")
    case x => (x, repo.length()-(x+1)) match{
      case (y, 0) => throw new Exception("Malformed Repo: The Repo does not exist. (Nothing found after /)")
      case (1, 1) => s"${repo.charAt(0)}/${repo.charAt(2)}"
      case (1, _) => s"${repo.charAt(0)}/${repo.substring(2, 4)}"
      case (y, 1) => s"${repo.substring(0,2)}/${repo.charAt(y+1)}"
      case (y, _) => s"${repo.substring(0,2)}/${repo.substring(y+1,y+3)}"
    }
  }

  def clone(repo: String, config: Config): JsObject = {
    val path = repoToPath(repo)
    fileSystemMap.get(I(s"$path/")) match{
      case None =>
        fileSystemMap.put(I(s"$path/")) // This shouldn't really work, since it puts in keys instead of data. Yay mutability! :|
        s"git clone https://github.com/$repo $startingDirectory/$path".!
      case Some(I("")) =>
        s"git clone https://github.com/$repo $startingDirectory/$path".!
      case Some(I(_)) => //Assume it's already cloned
        try {
          s"git -C $startingDirectory/$path pull".!
        } catch {
          case e: Exception => // The below should NEVER occur.
            return JsObject("status" -> JsString("error"),
              "exception" -> new JsStringGitException(CloneException(path, s"The path $path is not a Git Repo.")))
        }
    }
    JsObject(Map("status" -> JsString("ok")))
  }

  def pull(repo: String, config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    val path = repoToPath(repo)
    if (hasBeenCloned(path)){
        try{
          s"git -C $startingDirectory/$path pull".!
          JsObject(Map("status" -> JsString("ok")))
        } catch {
          case e: Exception => JsObject("status" -> JsString("error"),
            "exception" -> new JsStringGitException(PullException(repo, e.getMessage)))
        }
    } else{
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(PullException(repo, "Repo does not exist.")))
    }
  }

  def getCommits(repo: String, config: Config, sinceLastTime: Boolean = false, since: String = "", pattern: Option[String] = None): JsObject = {
    def sinceLogger(startingLog: String): String = since match{
      case "" => startingLog
      case _ => s"$startingLog --since=$since"
    }
    val startingDirectory = config.getString("repoLocation")
    val path = repoToPath(repo)
    if (hasBeenCloned(path)){
      try {
        val startingLog = s"git -C $startingDirectory/$path log"
        val sinceLog = if (sinceLastTime) {
          solrMapOpt match {
            case Some(solrMap) =>
              solrMap.get(path) match {
                case Some(gitRepo) => gitRepo.lastCommit match {
                  case Some(s) => s"$startingLog --since=$s"
                  case _ => sinceLogger(startingLog)
                }
                case None => sinceLogger(startingLog)
              }
            case _ => sinceLogger(startingLog)
          }
        } else sinceLogger(startingLog)
        val commitHashes = pattern match {
          case None => s"$sinceLog --pretty=format:%H".!!
          case Some(pat) => s"$sinceLog --pretty=format:%H -- $pat".!!
        }
        val commitArray = commitHashes.split("\n")
        (if (commitArray.nonEmpty){
            val lCom = s"git -C $startingDirectory/$path show --date=unix --format=%ad -s ${commitArray(0)}".!!
            solrMapOpt match {
              case Some(solrMap) =>
                solrMap.put(path, GitRepo(path, Some((lCom.split("\n")(0).toInt + 1).toString)))
              case None => ()
            }
            lCom
          } else "") match {
          case "" => JsObject("status" -> JsString("ok"),
            "results" -> JsArray(commitHashes.split("\n").toVector.map (JsString(_))))
          case lastCommit => JsObject("status" -> JsString("ok"),
            "results" -> JsArray(commitHashes.split("\n").toVector.map(JsString(_))),
            "date" -> JsString(lastCommit.split("\n")(0)))
        }
      } catch{
        case e: Exception => JsObject("status" -> JsString("error"),
          "exception" -> new JsStringGitException(CommitsException(repo, e.toString)))
      }
    } else{
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(CommitsException(repo, "Repo does not exist.")))
    }
  }

  def extractCommit(repo: String, hash: String, config: Config): JsObject = {
    def addNextToMap(message: String, extractionList: List[String], map: Map[String, JsString] = Map()): Map[String, JsString] = extractionList match {
      case Nil => map
      case head :: rest =>
        val splitVal = message.indexOf('\0')
        (splitVal, head) match{
          case (-1, _) => map + (head -> JsString(message)) // | (_, "diff") => map + (head -> JsString(message.substring(2))) //"\n\n"
          case _ => addNextToMap(message.substring(splitVal+1), rest, map + (head -> JsString(message.substring(0, splitVal))))
        }
    }
    val startingDirectory = config.getString("repoLocation")
    val path = repoToPath(repo)
    if (hasBeenCloned(path)){
      val commitExtract =  s"git -C $startingDirectory/$path show --date=unix --format=%an%x00%ae%x00%cn%x00%ce%x00%s%x00%b%x00%ad%x00 -s $hash".!!
      val extractionList = List("name", "email", "commitName", "commitEmail", "subject", "body", "date")
      JsObject("status" -> JsString("ok"), "results" -> JsObject(addNextToMap(commitExtract, extractionList)))
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(CommitInfoException(repo, hash, "Repo does not exist.")))
    }
  }

  def getFiles(repo: String, hash: String, pattern: Option[String], config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    val path = repoToPath(repo)
    if (hasBeenCloned(path)){
      val startingShow = s"git -C $startingDirectory/$path show --oneline --name-only $hash"
      val listOfFiles = pattern match{
        case None => s"$startingShow".!!
        case Some(pat) => s"$startingShow -- $pat".!!
      }
      JsObject("status" -> JsString("ok"), "results" -> JsArray(listOfFiles.split("\n").foldRight(List[JsString]()){
        case ("", list) => list
        case (str, list) => JsString(str) :: list
      }.tail.toVector))
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(CommitGetFileException(repo, hash, "Repo does not exist.")))
    }
  }

  def getFileContents(repo: String, hash: String, fileName: String, config: Config): JsObject = {
    val startingDirectory = config.getString("repoLocation")
    val path = repoToPath(repo)
    if (hasBeenCloned(path)) {
      val fileContents = s"git -C $startingDirectory/$path show $hash:$fileName".!!
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
    val path = repoToPath(repo)
    if (hasBeenCloned(path)){
      val res = JsArray(s"git -C $startingDirectory/$path show $hash -- $fileName".!!.split("\n").foldRight(List[JsString]()){
        case ("", jsArr) => jsArr
        case (line, jsArr) if line.length >= 2 && line.substring(0,2).equals("@@") => jsArr
        case (line, jsArr) => JsString(line) :: jsArr
      }.tail.toVector)
      //As for right now, I'm leaving it as a JsArray. I'm still slightly unsure on how patches work in git... :|
      JsObject("status" -> JsString("ok"), "results" -> res)
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(FilePatchException(repo, hash, fileName, "Repo does not exist.")))
    }
  }

  def getFileParents(repo: String, hash: String, fileName: String, config: Config): JsObject = {
    def getParents(lis: List[String]): JsArray = lis match {
      case Nil => JsArray(Vector())
      case "" :: rest => getParents(rest)
      case stuff :: _ => JsArray(stuff.split(" ").toList.tail.tail.map(JsString(_)).toVector)
    }
    val startingDirectory = config.getString("repoLocation")
    val path = repoToPath(repo)
    if (hasBeenCloned(path)) {
      //As for right now, I'm leaving it as a JsArray. I'm unsure how Ken actually uses this... :|
      val res = getParents(s"git -C $startingDirectory/$path log --full-history --parents $hash -- $fileName".!!.split("\n").foldRight(List[String]()){
        case ("", jsArr) => jsArr
        case (line, jsArr) => line :: jsArr
      })
      JsObject("status" -> JsString("ok"), "results" -> res)
    } else {
      JsObject("status" -> JsString("error"),
        "exception" -> new JsStringGitException(FileParentException(repo, hash, fileName, "Repo does not exist.")))
    }
  }

  def resetSolrMap(repo: String, all: Boolean = false): JsObject = {
    solrMapOpt match{
      case Some(solrMap) =>
        if (all){
          solrMap.extract()
          JsObject("status" -> JsString("ok"), "results" -> JsString("The SolrMap has been entirely wiped!"))
        } else{
          solrMap.remove(repoToPath(repo))
          JsObject("status" -> JsString("ok"), "results" -> JsString(s"Repo $repo has been removed from the SolrMap!"))
        }
      case None => JsObject("status" -> JsString("ok"), "results" -> JsString("WARNING: SolrMap not in use!"))
    }
  }
}
