
import java.io._
import scala.sys.process._

import pipecombi._
import com.typesafe.config.Config
import mthread_abstrac.ConfigHelper

import scala.io.{BufferedSource, Source}
import scala.util.parsing.json.JSON
import scalaj.http.Http


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

  override def apply(s: String): GitID = s.indexOf('/') match {
      case -1 => throw new Exception(s"The string $s doesn't match GitID.")
      case x =>
        val user = s.substring(0, x)
        val next = s.substring(x + 1)
        val endOfString = {
          val nextPortion = next.indexOf(':')
          if (nextPortion < 0) next.length else nextPortion
        }
        val repoHash = next.substring(0, endOfString)
        repoHash.indexOf('/') match {
          case -1 => GitID(user, repoHash, None)
          case y => GitID(user, repoHash.substring(0, y), Some(repoHash.substring(y + 1)))
        }
  }
}

case class GitRepo(gitID: GitID, repoPath: String) extends Identifiable {
  override def identity(): Identity = Identity(s"${gitID.identity.id}:$repoPath",None)
  override def apply(s: String): GitRepo = s.indexOf(':') match{
    case -1 => throw new Exception(s"The string $s doesn't match GitRepo.")
    case x =>
      val gID = gitID.apply(s.substring(0, x))
      GitRepo(gID, s.substring(x+1))
  }
}
/*
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
*/
// Mock Database Maps
/*
case class SolrDoc() extends Identifiable {
  override def identity(): Identity = ???
}
*/
case class SolrMap[SDoc <: Identifiable](name: String, template: SDoc, conf: Config = null) extends DataMap[SDoc](Some(template)) {
  val databaseLocation: String = ConfigHelper.possiblyInConfig(Some(conf), name+"Location", "http://localhost:8983/solr/")
  val collectionName: String = ConfigHelper.possiblyInConfig(Some(conf), name+"CollectionName","gettingstarted")
  val url: String = databaseLocation+collectionName+"/"
  /*
  val (verti, field) = if (ConfigHelper.possiblyInConfig(conf, name+"Vertical", default = true)){
    (true, ConfigHelper.possiblyInConfig(conf, name+"Field", name))
  } else {
    (false, ConfigHelper.possiblyInConfig(conf, name+"Document", name))
  }*/
  val fName: String = ConfigHelper.possiblyInConfig(Some(conf), name+"Field", name)
  val delimiter = "`#**#`"

  def checkDocument(list: List[Map[String, Any] @ unchecked], id: String): Map[String, Any] = list match{
    case Nil => Map.empty
    case (first: Map[String @ unchecked, Any @ unchecked]) :: last =>
      first.get("id") match{
        case Some(str: String) if str.equals(id) => first
        case _ => checkDocument(last, id)
      }
    case first :: last => checkDocument(last, id)
  }

  def getObject(k: String = ""): List[(String, Any)] = {
    val queryURL = url+"select?wt=json&q=id=\"" + k + "\""
    val json = Http(queryURL).asString.body //Query the Database Using the URL
    JSON.parseFull(json) match {
      case Some(parsed: Map[String@unchecked, Any@unchecked]) =>
        parsed.get("response") match {
          case Some(resp: Map[String@unchecked, Any@unchecked]) =>
            resp.get("docs") match {
              case Some(list: List[Map[String, Any] @ unchecked]) =>
                val first = checkDocument(list, k)
                first.foldRight(List.empty[(String, Any)]) {
                  case ((key, value), l) =>
                    (key, value) :: l
                }
              case _ => List.empty[(String, Any)]
            }
          case _ => List.empty[(String, Any)]
        }
      case _ => List.empty[(String, Any)]
    }
  }

  override def put(identity: Identity, item: SDoc): Boolean = {
    val startingURL = url+"update"
    val id = identity.version match{
      case Some(y) => identity.id+delimiter+y
      case None => identity.id
    }
    val itemF = item.getVersion() match{
      case Some(y) => item.getId()+delimiter+y
      case None => item.getId()
    }
    val jsonValue = getObject(id) match{
      case l if l.isEmpty =>
        s"""{
           |  "add": {
           |    "doc": {
           |      "id": "$id",
           |      "$fName": "$itemF"
           |    }
           |  },
           |  "commit": {}
           |}
         """.stripMargin
      case l =>
        val (mostOfString, needToAddValue) = l.foldLeft(
          """{
            |  "add": {
            |    "doc": {""".stripMargin, true) {
          case ((json, curr), (key, value)) => if (key.equals(fName)) {
            (json + s"""
                      |      "$fName": "$itemF",""".stripMargin, false)
          } else if (!key.equals("_version_")) {
            (json + """
                      |      """.stripMargin + "\"" + key + "\": " + (value match {
              case s: String => "\"" + value + "\""
              case vL: List[_] => val tBC = "[ " +  vL.foldLeft(""){
                case (j: String, s: String) => j + "\"" + s + "\", "
                case (j: String, va) => j + va.toString + ", "
              }
                tBC.substring(0,tBC.length-2) + " ]"
              case _ => value.toString
            }) + ",", curr)
          } else{
            (json,curr)
          }
        }
        val mostOfString2 = if (needToAddValue){
          mostOfString + """
                           |      """.stripMargin + "\"" + fName + "\": \"" + itemF + "\","
        } else {
          mostOfString + ""
        }
        mostOfString2.substring(0, mostOfString2.length-1) +
          """
            |    }
            |  },
            |  "commit": {}
            |}
          """.stripMargin
    }
    //Find a way to POST Request this into Solr
    val json = Http(url+"update").postData(jsonValue.getBytes).header("Content-Type", "application/json").asString.body
    JSON.parseFull(json) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("error") match{
          case Some(_) => false
          case _ => true
        }
      case _ => false
    }
  }

  def getSDoc(s: String): SDoc = {
    //new SDoc(s)
    s.indexOf(delimiter) match{
      case -1 => template.apply(s).asInstanceOf[SDoc]
      case x => template.apply(s.substring(0, x)).asInstanceOf[SDoc]
    }
  }

  def getIdentity(s: String): Identity = {
    s.indexOf(delimiter) match{
      case -1 => Identity(s, None)
      case x => Identity(s.substring(0,x), Some(s.substring(x+delimiter.length)))
    }
  }

  override def get(identity: Identity): Option[SDoc] = {
    val id = identity.version match{
      case Some(y) => identity.id+delimiter+y
      case None => identity.id
    }
    val queryURL = url+"select?wt=json&q=id=\"" + id + "\"%20AND%20" + name + ":[*%20TO%20*]"
    val json = Http(queryURL).asString.body //Query the Database Using the URL
    JSON.parseFull(json) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("response") match{
          case Some(resp: Map[String @ unchecked, Any @ unchecked]) =>
            resp.get("docs") match{
              case Some(resp2: List[Map[String, Any] @ unchecked]) => resp2 match{
                case first :: list =>
                  first.get(fName) match{
                    case Some(List(x)) => Some(getSDoc(x.toString))
                    case Some(x :: more) => Some(getSDoc(x.toString))
                    case Some(x) => Some(getSDoc(x.toString))
                    case None => None
                  }
                case _ => None
              }
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
  }

  override def identities: List[Identity] = {
    val queryURL = url+"select?wt=json&rows=1000000&q="+name+":[*%20TO%20*]"
    val json: String = Http(queryURL).asString.body //Find a way to Query the Database using a URL
    JSON.parseFull(json) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("response") match {
          case Some(resp: Map[String@unchecked, Any@unchecked]) =>
            resp.get("docs") match {
              case Some(resp2: List[Map[String, Any]@unchecked]) =>
                resp2.foldRight(List.empty[Identity]) {
                  case (map, l) => map.get("id") match {
                    case Some(List(v)) => map.get(fName) match{
                      case Some(List(v2)) =>
                        getIdentity(v.toString) :: l
                      case Some(v2 :: more) => getIdentity(v.toString) :: l
                      case Some(v2) => getIdentity(v.toString) :: l
                    }
                    case Some(v) => map.get(fName) match{
                      case Some(List(v2)) =>
                        getIdentity(v.toString) :: l
                      case Some(v2 :: more) => getIdentity(v.toString) :: l
                      case Some(v2) => getIdentity(v.toString) :: l
                    }
                    case _ => l
                  }
                }
              case _ => List.empty[Identity]
            }
          case _ => List.empty[Identity]
        }
      case _ => List.empty[Identity]
    }
  }

  override def items: List[SDoc] = {
    val queryURL = url+"select?wt=json&rows=1000000&q="+name+":[*%20TO%20*]"
    val json: String = Http(queryURL).asString.body //Find a way to Query the Database using a URL
    JSON.parseFull(json) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("response") match {
          case Some(resp: Map[String@unchecked, Any@unchecked]) =>
            resp.get("docs") match {
              case Some(resp2: List[Map[String, Any]@unchecked]) =>
                resp2.foldRight(List.empty[SDoc]) {
                  case (map, l) => map.get("id") match {
                    case Some(List(v)) => map.get(fName) match{
                      case Some(List(v2)) => getSDoc(v2.toString) :: l
                      case Some(v2 :: more) => getSDoc(v2.toString) :: l
                      case Some(v2) => getSDoc(v2.toString) :: l
                    }
                    case _ => l
                  }
                }
              case _ => List.empty[SDoc]
            }
          case _ => List.empty[SDoc]
        }
      case _ => List.empty[SDoc]
    }
  }

  override def displayName: String = name
}

case class TextMap[GDoc <: Identifiable](name: String, template: GDoc) extends DataMap[GDoc](Some(template)) {
  val file = new File(name)
  override def put(item: GDoc): Boolean = put(item.identity(), item)

  override def put(identity: Identity, item: GDoc): Boolean = {
    try {
      val writer = new BufferedWriter(new FileWriter(file, true))
      get(identity) match {
        case None =>
          writer.append(s"${identity.id.toString}\n")
          writer.close()
          true
        case Some(x) =>
          true
      }
    }
    catch{
      case e: Exception => false
    }
  }

  override def get(identity: Identity): Option[GDoc] = {
    try {
      val src: BufferedSource = Source.fromFile(file)
      val id = identity
      def checkIfInFile(lines: List[String]): Option[GDoc] = lines match{
        case Nil => None
        case line :: rest =>
          if (line.equals(identity.id))
            Some(template.apply(line).asInstanceOf[GDoc])
          else checkIfInFile(rest)
      }
      val returnValue = checkIfInFile(src.getLines().toList)
      returnValue
    } catch {
      case e: Exception => None
    }
  }

  override def identities: List[Identity] = {
    val src: BufferedSource = Source.fromFile(file)
    src.getLines().toList.foldRight(List.empty[Identity]){
      case (str, list) => Identity(str, None) :: list
    }
  }

  override def items: List[GDoc] = {
    val src: BufferedSource = Source.fromFile(file)
    src.getLines().toList.foldRight(List.empty[GDoc]){
      case (str, list) => template.apply(str).asInstanceOf[GDoc] :: list
    }
  }
}

case class FileSystemMap[GDoc <: Identifiable](subdirectory: String, template: GDoc) extends DataMap[GDoc](Some(template)) {
  override def put(identity: Identity, item: GDoc): Boolean = {
    try {
      def mkDirs(path: String, currPath: String = ""): Unit = {
        path.indexOf("/") match {
          case -1 => ()
          case 1 if path.charAt(0) == '/' => ()
          case 2 if path.substring(0,2).equals("..") => ()
          case x =>
            val file = new File(s"$currPath${path.substring(0,x)}")
            file.mkdir()
            mkDirs(path.substring(x+1), s"$currPath${path.substring(0,x+1)}")
        }
      }
      mkDirs(s"$subdirectory/${identity.id}")
      val writer = new BufferedWriter(new FileWriter(s"$subdirectory/${identity.id}", false))
      writer.write(item.identity().id)
      writer.close()
      true
    }
    catch{
      case e: Exception => false
    }
  }

  override def get(identity: Identity): Option[GDoc] = {
    try {
      if (new File(s"$subdirectory/${identity.id}").exists()){
        val str = Source.fromFile(s"$subdirectory/${identity.id}").getLines().foldRight(""){
          case (line, fileContent) => s"$line\n$fileContent"
        }
        Some(template.apply(str).asInstanceOf[GDoc])
      } else {
        None
      }
    }
    catch {
      case e: Exception => None
    }
  }

  override def identities: List[Identity] = {
    def getFilesOutOfSubdirectory(name: String, prefix: String = ""): List[Identity] = {
      def file = new File(name)
      file.listFiles().toList.foldRight(List.empty[Identity]){
        case (fl, list) =>
          val fileName = fl.getName
          if (fl.isDirectory) getFilesOutOfSubdirectory(s"$name/$fileName", s"$prefix$fileName/") ::: list
          else Identity(s"$prefix$fileName", None) :: list
      }
    }
    getFilesOutOfSubdirectory(subdirectory)
  }

  override def items: List[GDoc] = {
    def getStuffOutOfSubdirectory(name: String, start: String = ""): List[GDoc] = {
      //WARNING: This could break the JVM with an OOM error, and probably will with large files.
      //Use at own risk.
      def file = new File(name)
      file.listFiles().toList.foldRight(List.empty[GDoc]) {
        case (fl, list) =>
          val fileName = fl.getName
          if (fl.isDirectory && start.equals("")) getStuffOutOfSubdirectory(s"$name/$fileName", name) ::: list
          else if (fl.isDirectory) getStuffOutOfSubdirectory(s"$name/$fileName", start) ::: list
          else {
            (if (start.equals("")) get(Identity(fileName, None))
            else get(Identity(s"${name.substring(start.length)}/$fileName", None))) match {
              case Some(x) => x :: list
              case None => list
            }
          }
      }
    }
    getStuffOutOfSubdirectory(subdirectory)
  }
}

// Mock feature transformers

case class Clone(str: String = "") extends IncrTransformer[GitID, GitRepo](str) {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap", Done)
  override val provMap = SolrMap[Identity]("ProvMap", Identity("", None))
  override val errMap = SolrMap[ErrorSummary]("ErrorMap", GeneralErrorSummary(new Exception("")))

  override def compute(input: GitID): List[GitRepo] = {
    val repoLocation = input.identity().id
    val repos = new File("repos")
    if (!repos.isDirectory) repos.mkdir()
    val secondRepos = new File(s"repos/${input.user}")
    if (!secondRepos.isDirectory) secondRepos.mkdir()
    val thirdRepos = new File(s"repos/${input.user}/${input.repo}")
    if (!(thirdRepos.isDirectory && thirdRepos.listFiles().length > 0))
      s"git clone https://github.com/$repoLocation repos/${input.user}/${input.repo}".!
    List(GitRepo(input, s"repos/${input.user}/${input.repo}"))
  }
}

case class CommitExtraction(str: String = "") extends IncrTransformer[GitRepo, GitRepo](str) {
  override val version = "0.1"
  override val statMap = SolrMap[Stat]("StatMap", Done)
  override val provMap = SolrMap[Identity]("ProvMap", Identity("", None))
  override val errMap = SolrMap[ErrorSummary]("ErrorMap", GeneralErrorSummary(new Exception("")))

  override def compute(input: GitRepo): List[GitRepo] = {
    println(s"I have started with $input.")
    println(s"git -C ${input.repoPath} log --pretty=format:%H")
    val lisCommits = s"git -C ${input.repoPath} log --pretty=format:%H".!!
    println(lisCommits)
    
    ???
  }
}
/*
case class Build() extends IncrTransformer[GitRepo, GitBuilds] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: GitRepo): List[GitBuilds] = ???
}

case class CallbackInstr() extends IncrTransformer[GitBuilds,InstrumentedAPKs] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: GitBuilds): List[InstrumentedAPKs] = ???
}

case class ExtractGroum() extends IncrTransformer[GitBuilds,Groums] {
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
*/
object Fixr {

  //def Stop[A <: Identifiable](): IncrTransformer[A,End] = Stop[A]()

  def Loop[A <: Identifiable](): IncrTransformer[A,A] = Loop[A]()

}

// The Pipeline
/*
class MockFixrPipeline {

  // Pipeline Input Features
  val gitIds = SolrMap[GitID]("GitIDs", GitID("","",None))

  // Computed Features
  val gitRepos = SolrMap[GitRepo]("GitRepos", GitRepo(GitID("","",None), ""))
  /*val gitBuilds = SolrMap[GitBuilds]("GitBuilds")
  val instrAPKs = SolrMap[InstrumentedAPKs]("InstrAPKs")
  val groums    = SolrMap[Groums]("Groums")

  val end = SolrMap[End]("End")
  */

  // Pipeline
  // {gitIds :--{ Clone--> gitRepos } } :--Build--> gitBuilds

  // gitBuilds :--CallbackInstr--> instrAPKs

  // gitBuilds :--ExtractGroum--> groums




  import Implicits._
  /*
  val fixr = (gitIds :--Clone()--> gitRepos :--Build()--> gitBuilds) :<  {
    (CallbackInstr()--> instrAPKs :--Loop[InstrumentedAPKs]--> instrAPKs :--Stop[InstrumentedAPKs]--> end) ~
    (ExtractGroum()--> groums :--Loop[Groums]--> groums)
  }


  fixr
  */

  /*
  || {
    (CallbackInstr--> instrAPKs :--Stop[InstrumentedAPKs]()--> end) ~
    (ExtractGroum--> groums :--Stop[Groums]()--> end)
  } */
}
*/

object Test{
  def main(args: Array[String]): Unit = {
    import Implicits._
    val repos = TextMap("first50.txt", GitID("", "", None))
    val cloned = SolrMap("GitRepos", GitRepo(GitID("", "", None), ""))
    val whatsNext = SolrMap("NoClue", GitRepo(GitID("", "", None), ""))
    val pipe = repos :--Clone()--> cloned :--CommitExtraction()--> whatsNext
    pipe.run("akka")
  }
}