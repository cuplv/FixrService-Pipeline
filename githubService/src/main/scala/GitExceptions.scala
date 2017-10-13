import spray.json.JsString

/**
  * Created by chanceroberts on 9/26/17.
  */
abstract class GitException(reason: String) extends RuntimeException(reason)

case class GitServiceException(command: String, format: String)
  extends GitException(s"JSON file for $command malformed. It should look like $format.")

object CloneException{
  def createNewCloneException(repo: String, reason: String, prevEx: Option[CloneException] = None): CloneException = prevEx match{
    case None => CloneException(repo, reason)
    case Some(CloneException(rpo, rson)) => CloneException(s"$rpo: $rson, $repo", reason)
  }
}

class JsStringGitException(gitException: GitException) extends JsString(gitException.getMessage)

case class CloneException(repo: String, reason: String) extends GitException(s"Problem in cloning $repo: $reason")

case class PullException(repo: String, reason: String) extends GitException(s"Problem in pulling $repo: $reason")

case class CommitsException(repo: String, reason: String) extends GitException(s"Problem in getting the commits of $repo: $reason")

case class CommitInfoException(repo: String, hash: String, reason: String)
  extends GitException(s"Problem in extracting info from commit $repo/$hash: $reason")

case class CommitGetFileException(repo: String, hash: String, reason: String)
  extends GitException(s"Problem in getting the files from the commit $repo/$hash: $reason")

case class FileGetContentsException(repo: String, hash: String, fileName: String, reason: String)
  extends GitException(s"Problem in getting $fileName's content from the commit $repo/$hash: $reason")

case class FilePatchException(repo: String, hash: String, fileName: String, reason: String)
  extends GitException(s"Problem in getting $fileName's patch information from the commit $repo/$hash")

case class FileParentException(repo: String, hash: String, fileName: String, reason: String)
  extends GitException(s"Problem in getting $fileName's parent information from the commit $repo/$hash")
