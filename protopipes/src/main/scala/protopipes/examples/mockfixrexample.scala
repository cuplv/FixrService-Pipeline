package protopipes.examples

import java.io.File

import scala.sys.process._
import protopipes.computations.Mapper
import protopipes.configurations.{DataStoreBuilder, PipeConfig}
import protopipes.data.serializers.JsonSerializer
import protopipes.data.{BasicIdentity, I, Identifiable, Identity}
import protopipes.exceptions.UserComputationException
import protopipes.store.instances.file.TextFileDataMap
import protopipes.store.instances.solr.SolrDataMap
import spray.json._

/**
  * Created by chanceroberts on 8/31/17.
  */
case class GitID(user: String, repo: String) extends Identifiable[GitID]{
  override def mkIdentity(): Identity[GitID] = BasicIdentity(s"$user/$repo")
}

object MockProtocol extends DefaultJsonProtocol {
  implicit val gitID: JsonFormat[GitID] = jsonFormat2(GitID)
  implicit val gitRepo: JsonFormat[GitRepo] = jsonFormat3(GitRepo)
}

object GitIDSerializer extends JsonSerializer[GitID] {
  import MockProtocol._
  override def serializeToJson_(d: GitID): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): GitID = json.convertTo[GitID]
}

case class GitRepo(user: String, repo: String, repoPath: String) extends Identifiable[GitRepo]{
  override def mkIdentity(): Identity[GitRepo] = BasicIdentity(s"$user/$repo:$repoPath")
}

object GitRepoSerializer extends JsonSerializer[GitRepo]{
  import MockProtocol._
  override def serializeToJson_(d: GitRepo): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): GitRepo = json.convertTo[GitRepo]
}

case class StringToGitID() extends Mapper[I[String], GitID]{
  override def compute(input: I[String]): List[GitID] = {
    val slash = input.a.indexOf('/')
    List(GitID(input.a.substring(0, slash), input.a.substring(slash+1)))
  }
}

case class Clone(repoFolderLocation: String = "mockfixrexample/repos") extends Mapper[GitID, GitRepo]{
  override def compute(input: GitID): List[GitRepo] = {
    try {
      val repos = new File(repoFolderLocation)
      if (!repos.exists) repos.mkdir()
      val userRepos = new File(s"$repoFolderLocation/${input.user}")
      if (!userRepos.exists) userRepos.mkdir()
      val repoLocation = s"${input.user}/${input.repo}"
      val trueRepo = new File(s"$repoFolderLocation/$repoLocation")
      if (!(trueRepo.isDirectory && trueRepo.length() > 0)) {
        s"git clone https://github.com/$repoLocation $repoFolderLocation/$repoLocation".!
        List(GitRepo(input.user, input.repo, repoLocation))
      } else
        throw new UserComputationException("Repo that should be cloned has stuff in it.", None)
    } catch{
      case u: UserComputationException => throw u
      case e: Exception => throw new UserComputationException("File System is not formatted correctly?", Some(e))
    }
  }
}


object mockfixrexample {
  def main(args: Array[String]): Unit = {
    val config = PipeConfig.newConfig()
    val textMap = new TextFileDataMap("src/main/mockfixrexample/first50.txt")
    val storeBuilder = DataStoreBuilder.load(config)
    val gitID = new SolrDataMap[GitID, GitID](GitIDSerializer, "GitIDs")
    val clonedMap = new SolrDataMap[GitRepo, GitRepo](GitRepoSerializer, "GitRepos")
    import protopipes.pipes.Implicits._
    val pipe = textMap :--StringToGitID()-->gitID
  }
}
