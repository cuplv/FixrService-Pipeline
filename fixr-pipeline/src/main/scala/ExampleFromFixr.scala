import bigglue.data.{BasicIdentity, Identifiable, Identity}

/**
  * Created by chanceroberts on 12/14/17.
  */

case class GitID(user: String, repo: String) extends Identifiable[GitID]{
  override def mkIdentity(): Identity[GitID] = BasicIdentity(s"$user/$repo")
}

case class GitRepo(gitID: GitID, hash: String) extends Identifiable[GitRepo]{
  override def mkIdentity(): Identity[GitRepo] = BasicIdentity(s"${gitID.getId()}/$hash")
}



object ExampleFromFixr {
  def main(args: Array[String]): Unit = {

  }
}
