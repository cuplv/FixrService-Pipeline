package protopipes.examples

import protopipes.computations.Mapper
import protopipes.data.{BasicIdentity, Identifiable, Identity}

/**
  * Created by chanceroberts on 8/31/17.
  */
case class GitID(user: String, repo: String) extends Identifiable[GitID]{
  override def mkIdentity(): Identity[GitID] = BasicIdentity(s"$user/$repo")
}

case class GitRepo(user: String, repo: String, repoPath: String) extends Identifiable[GitRepo]{
  override def mkIdentity(): Identity[GitRepo] = BasicIdentity(s"$user/$repo:$repoPath")
}

case class Clone() extends Mapper[GitID, GitRepo]{
  override def compute(input: GitID): List[GitRepo] = ???
}

object mockfixrexample {
  def main(args: Array[String]): Unit = {

  }
}
