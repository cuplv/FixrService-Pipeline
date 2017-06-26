import pipecombi._

/**
  * Created by edmundlam on 6/23/17.
  */


// Mock Feature Declarations

case class GitID(user: String, repo: String, hashOpt: Option[String]) extends Identifiable {
  override def identity(): Identity = {
    val hashStr = hashOpt match {
      case Some(hash) => s"/$hash"
      case None => ""
    }
    Identity(s"$user/$repo$hashStr",None)
  }
}

case class GitRepo(gitID: GitID, repoPath: String) extends Identifiable {
  override def identity(): Identity = Identity(s"${gitID.identity.id}:$repoPath",None)
}

case class GitBuilds(gitRepo: GitRepo, buildPath: String) extends Identifiable {
  override def identity(): Identity = Identity(s"${gitRepo.identity.id}:$buildPath", None)
}

case class InstrumentedAPKs(gitID: GitID, apkPath: String) extends Identifiable {
  override def identity(): Identity = Identity(s"${gitID.identity.id}:$apkPath",None)
}

case class Groums(gitID: GitID, methodName: String, dot: String) extends Identifiable {
  override def identity(): Identity = ???
}

case class End() extends Identifiable {
  override def identity(): Identity = Identity("", None)
}

// Mock Database Maps

case class SolrDoc() extends Identifiable {
  override def identity(): Identity = ???
}

case class SolrMap[SDoc <: Identifiable](name: String) extends DataMap[SDoc] {
  override def put(identity: Identity, item: SDoc): Boolean = ???
  override def get(identity: Identity): Option[SDoc] = ???
  override def identities: List[Identity] = ???
  override def items: List[SDoc] = ???
  override def displayName: String = name
}

// Mock feature transformers

object Clone extends IncrTransformer[GitID, GitRepo] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: GitID): List[GitRepo] = ???
}

object Build extends IncrTransformer[GitRepo, GitBuilds] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: GitRepo): List[GitBuilds] = ???
}

object CallbackInstr extends IncrTransformer[GitBuilds,InstrumentedAPKs] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: GitBuilds): List[InstrumentedAPKs] = ???
}

object ExtractGroum extends IncrTransformer[GitBuilds,Groums] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: GitBuilds): List[Groums] = ???
}

case class Stop[A <: Identifiable]() extends IncrTransformer[A,End] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: A): List[End] = List()
}

case class Loop[A <: Identifiable]() extends IncrTransformer[A,A] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: A): List[A] = List(input)
}

object Fixr {

  def Stop[A <: Identifiable](): IncrTransformer[A,End] = Stop[A]()

  def Loop[A <: Identifiable](): IncrTransformer[A,A] = Loop[A]()

}

// The Pipeline

class MockFixrPipeline {

  // Pipeline Input Features
  val gitIds = SolrMap[GitID]("GitIDs")

  // Computed Features
  val gitRepos = SolrMap[GitRepo]("GitRepos")
  val gitBuilds = SolrMap[GitBuilds]("GitBuilds")
  val instrAPKs = SolrMap[InstrumentedAPKs]("InstrAPKs")
  val groums    = SolrMap[Groums]("Groums")

  val end = SolrMap[End]("End")

  // Pipeline
  // {gitIds :--{ Clone--> gitRepos } } :--Build--> gitBuilds

  // gitBuilds :--CallbackInstr--> instrAPKs

  // gitBuilds :--ExtractGroum--> groums



  import Implicits._

  val fixr = (gitIds :--Clone--> gitRepos :--Build--> gitBuilds) :<  {
    (CallbackInstr--> instrAPKs :--Loop[InstrumentedAPKs]--> instrAPKs :--Stop[InstrumentedAPKs]--> end) ~
    (ExtractGroum--> groums :--Loop[Groums]--> groums)
  }

  fixr

  /*
  || {
    (CallbackInstr--> instrAPKs :--Stop[InstrumentedAPKs]()--> end) ~
    (ExtractGroum--> groums :--Stop[Groums]()--> end)
  } */
}
