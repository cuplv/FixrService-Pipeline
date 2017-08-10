
import java.io._
import java.util.Base64
import scala.sys.process._

import pipecombi._
import com.typesafe.config.Config
import mthread_abstrac.ConfigHelper
import feature.feature.BatchRequest

import scala.io.{BufferedSource, Source}
import scala.util.parsing.json.JSON
import scalaj.http.Http
//import feature.FeatureOuterClass

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

case class GitCommitInfo(gitRepo: GitRepo) extends Identifiable {
  val dMap = SolrDocMap("!" + gitRepo.identity().id, Identity("", None))
  override def identity(): Identity = Identity("!"+gitRepo.identity().id, gitRepo.identity().version)
  override def apply(s: String): GitCommitInfo = s.indexOf('!') match {
    case 0 => if (s.charAt(1) != '!') GitCommitInfo(gitRepo(s.substring(1)))
    else throw new Exception(s"The string $s doesn't match GitCommitInfo.")
    case _ => throw new Exception(s"The string $s doesn't match GitCommitInfo.")
  }
  def putInDataMap(map: Map[String, String]): Unit = map.foreach{
    case (field, value) => dMap.put(Identity(field, None), Identity(value, None))
  }
}

case class GitFeatures(gitRepo: GitRepo, featureInfo: feature.feature.Features) extends Identifiable {
  val id: String = Base64.getEncoder.encodeToString(featureInfo.toByteArray)
  override def identity(): Identity = Identity(s"!!${gitRepo.identity().id}!$id", None)
  override def apply(s: String): Identifiable = s.indexOf("!!") match {
    case 0 =>
      val split = s.substring(2).indexOf('!')+2
      GitFeatures(gitRepo(s.substring(2,split)), feature.feature.Features.parseFrom(Base64.getDecoder.decode(s.substring(split+1))))
    case _ => throw new Exception(s"The string $s doesn't match GitFeatures")
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
case class SolrDocMap[Field <: Identifiable](name: String, template: Field, conf: Config = null) extends DataMap[Field](Some(template)) {
  val databaseLocation: String = ConfigHelper.possiblyInConfig(Some(conf), name+"Location", "http://localhost:8983/solr/")
  val collectionName: String = ConfigHelper.possiblyInConfig(Some(conf), name+"CollectionName","gettingstarted")
  val documentID: String = ConfigHelper.possiblyInConfig(Some(conf), name+"DocumentID", name)
  val url: String = databaseLocation+collectionName+"/"
  val delimiter = "`#**#`"

  override def put(identity: Identity, item: Field): Boolean = {
    val fName = identity.version match{
      case None => identity.id
      case Some(x) => identity.id + delimiter + identity.version.get
    }
    val itemF = item.identity().version match{
      case None => item.identity().id
      case Some(x) => item.identity().id + delimiter + item.identity().version.get
    }
    val jsonValue = getObject match{
      case Nil =>
        s"""{
           |  "add": {
           |    "doc": {
           |      "id": "$documentID",
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
    val json = Http(url+"update").postData(jsonValue.getBytes).header("Content-Type", "application/json").asString.body
    JSON.parseFull(json) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("error") match{
          case Some(x) => false
          case _ => true
        }
      case _ => false
    }
  }

  def getObject: List[(String, Any)] = {
    val queryURL = url+"select?wt=json&q=id=\"" + documentID + "\""
    val json = Http(queryURL).asString.body //Query the Database Using the URL
    JSON.parseFull(json) match {
      case Some(parsed: Map[String@unchecked, Any@unchecked]) =>
        parsed.get("response") match {
          case Some(resp: Map[String@unchecked, Any@unchecked]) =>
            resp.get("docs") match {
              case Some(list: List[Map[String, Any] @ unchecked]) =>
                val first = checkDocument(list, documentID)
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

  def checkDocument(list: List[Map[String, Any] @ unchecked], id: String): Map[String, Any] = list match{
    case Nil => Map.empty
    case (first: Map[String @ unchecked, Any @ unchecked]) :: last =>
      first.get("id") match{
        case Some(str: String) if str.equals(id) => first
        case _ => checkDocument(last, id)
      }
    case first :: last => checkDocument(last, id)
  }

  override def get(identity: Identity): Option[Field] = {
    val queryURL = url+"select?wt=json&q=id=\"" + documentID + "\""
    val json = Http(queryURL).asString.body
    JSON.parseFull(json) match {
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("response") match {
          case Some(resp: Map[String @unchecked, Any @unchecked]) =>
            resp.get("docs") match {
              case Some(list: List[Map[String, Any] @unchecked]) =>
                val first = checkDocument(list, documentID)
                first.get(identity.id) match{
                  case Some(List(x)) => Some(template.apply(x.toString).asInstanceOf[Field])
                  case Some(x :: more) => Some(template.apply(x.toString).asInstanceOf[Field])
                  case Some(x) => Some(template.apply(x.toString).asInstanceOf[Field])
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
    getObject.foldRight(List.empty[Identity]){
      case ((fName, value), list) => fName.indexOf(delimiter) match{
        case -1 => Identity(fName, None) :: list
        case x => Identity(fName.substring(0,x), Some(fName.substring(x+delimiter.length))) :: list
      }
    }
  }

  override def items: List[Field] = {
    getObject.foldRight(List.empty[Field]){
      case ((fName, value), list) => template.apply(value.toString).asInstanceOf[Field] :: list
    }
  }
}

case class SolrMap[SDoc <: Identifiable](name: String, template: SDoc, conf: Config = null) extends DataMap[SDoc](Some(template)) {
  val databaseLocation: String = ConfigHelper.possiblyInConfig(Some(conf), name+"Location", "http://localhost:8983/solr/")
  val collectionName: String = ConfigHelper.possiblyInConfig(Some(conf), name+"CollectionName","gettingstarted")
  val url: String = databaseLocation+collectionName+"/"
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
              case s: String => "\"" + s.replaceAll("\"", "\\\"") + "\""
              case vL: List[_] => val tBC = "[ " +  vL.foldLeft(""){
                case (j: String, s: String) =>
                  j + "\"" + s.replaceAll("\\\"", "\\\\\"") + "\", "
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
          case Some(x) => println(x); false
          case _ => true
        }
      case x => println(x); false
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
    val queryURL = url+"select?wt=json&rows=1000&q=id=\"" + id + "\"%20AND%20" + name + ":[*%20TO%20*]"
    val json = Http(queryURL).asString.body //Query the Database Using the URL
    JSON.parseFull(json) match{
      case Some(parsed: Map[String @ unchecked, Any @ unchecked]) =>
        parsed.get("response") match{
          case Some(resp: Map[String @ unchecked, Any @ unchecked]) =>
            resp.get("docs") match{
              case Some(resp2: List[Map[String, Any] @ unchecked]) => resp2 match{
                case first :: list =>
                  val doc = checkDocument(first :: list, id)
                  doc.get(fName) match{
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

case class CommitExtraction(str: String = "") extends IncrTransformer[GitRepo, GitCommitInfo](str) {
  override val version = "0.1"
  override val statMap = SolrMap[Stat]("StatMap", Done)
  override val provMap = SolrMap[Identity]("ProvMap", Identity("", None))
  override val errMap = SolrMap[ErrorSummary]("ErrorMap", GeneralErrorSummary(new Exception("")))

  override def compute(input: GitRepo): List[GitCommitInfo] = {
    //s"git -C ${input.repoPath} pull".!
    println(s"Starting with ${input.identity().id}!")
    val lisCommits = s"git -C ${input.repoPath} log --pretty=format:%H".!!
    lisCommits.split("\n").toList.foldRight(List.empty[GitCommitInfo]){
      case (commit, listOfCommits) =>
        val commitInfo = s"git -C ${input.repoPath} show --pretty=fuller --name-only $commit".!!.split("\n")
        //Stupid way of doing this. Fix later!
        val comm = commitInfo(0).substring("commit ".length)
        val checkForMerges = commitInfo(1) match{
          case s if s.substring(0,7).equals("Merge: ") => 1
          case _ => 0
        }
        val (author, authorEmail) = commitInfo(1+checkForMerges).substring("Author:     ".length) match{
          case s => s.indexOf('<') match{
            case -1 => (s, "")
            case x => (s.substring(0, x-1), s.substring(x))
          }
        }
        val authorDate = commitInfo(2+checkForMerges).substring("AuthorDate: ".length)
        val (blame, blameEmail) = commitInfo(3+checkForMerges).substring("Commit:     ".length) match{
          case s => s.indexOf('<') match{
            case -1 => (s, "")
            case x => (s.substring(0, x-1), s.substring(x))
          }
        }
        val commitDate = commitInfo(4+checkForMerges).substring("CommitDate: ".length)
        val startMap = Map[String, String]("commit"->comm, "author"->author, "authorEmail"->authorEmail,
        "authorDate"->authorDate, "commiter"->blame, "commiterEmail"->blameEmail)
        def findCommitMessage(message: Array[String], line: Int, currString: String = ""): (String, Int) = if (message.length > line) message(line) match {
          case s if s.length > 4 && s.substring(0, 4).equals("    ") => currString match {
            case "" => findCommitMessage(message, line + 1, message(line).substring(4))
            case _ => findCommitMessage(message, line + 1, s"$currString\n${message(line).substring(4)}")
          }
          case _ => (currString, line)
        } else (currString, line)
        def findFiles(message: Array[String], line: Int, files: String = "[ "): String = if (message.length > line) message(line) match{
          case s if s.length > 4 && s.substring(0,4).equals("    ") => findFiles(message, line+2) //Should never happen, but just in case...
          case "" => findFiles(message, line+1, files)
          case x => findFiles(message, line+1, files + (files match{
            case "[ " => "\\\"" + x + "\\\""
            case _ => ", \\\""  + x + "\\\""
          }))
        } else files + " ]"
        val gID = input.gitID
        val gCI = GitCommitInfo(GitRepo(GitID(gID.user, gID.repo, Some(comm)), input.repoPath))
        val (title, nextLine) = findCommitMessage(commitInfo, 6+checkForMerges)
        val (message, lineAfter) = findCommitMessage(commitInfo, nextLine+1)
        val titleMap = startMap + ("title" -> title)
        val messageMap = titleMap + ("message" -> message)
        val filesMap = messageMap + ("files" -> findFiles(commitInfo, lineAfter))
        gCI.putInDataMap(filesMap)
        println(s"Finished with ${gCI.identity()}!")
        gCI :: listOfCommits
    }
  }
}

case class FeatureExtraction(str: String = "") extends IncrTransformer[GitCommitInfo, GitFeatures](str) {
  override val version = "0.1"
  override val statMap = SolrMap[Stat]("StatMap", Done)
  override val provMap = SolrMap[Identity]("ProvMap", Identity("", None))
  override val errMap = SolrMap[ErrorSummary]("ErrorMap", GeneralErrorSummary(new Exception("")))
  override def compute(input: GitCommitInfo): List[GitFeatures] = {
    val files = input.dMap.get(Identity("files", None)) match{
      case Some(i: Identity) => i.id
      case _ => ""
    }
    println(s"Starting with ${input.identity().id}!")
    val listOfFiles = JSON.parseFull(files) match{
      case Some(l: List[String @ unchecked]) => l
      case _ => List()
    }
    println(listOfFiles)
    listOfFiles.foldRight(List.empty[GitFeatures]){
      case (file, list) =>
        val len = file.length
        file.substring(len-5, len) match{
          case ".java" =>
            val f = s"git -C ${input.gitRepo.repoPath} show ${input.gitRepo.gitID.hashOpt.get}:$file".!!
            val fEncoded = Base64.getEncoder.encodeToString(f.getBytes())
            val data = "{ \"name\": \"" + file + "\", \"src\": \"" + fEncoded + "\" }"
            val json = Http("http://52.15.135.195:9002/features/single/json").postData("{ \"name\": \"" + file + "\", \"src\": \"" + fEncoded + "\" }").asString.body
            JSON.parseFull(json) match {
              case Some(m: Map[String@unchecked, Any@unchecked]) => m.get("status") match {
                case Some("ok") => m.get("output") match {
                  case Some(bytes: String) =>
                    val decodedBytes = Base64.getDecoder.decode(bytes)
                    //decodedBytes.foreach(print(_))
                    val features = feature.feature.Features.parseFrom(decodedBytes)
                    GitFeatures(input.gitRepo, features) :: list
                  case _ => throw new Exception("Output didn't match expected output.")
                }
                case Some(s: String) if s.substring(0, 5).equals("error") => m.get("output") match {
                  case Some(exception: String) => println(new Exception(exception)); list
                  case _ => println(new Exception(s.substring(6))); list
                }
                case _ => throw new Exception("Invalid status code.")
              }
              case _ => throw new Exception("Returned values on Feature Extraction are not in the correct format.")
            }
          case _ => list
        }
    }
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
    val templateID = GitID("", "", None)
    val templateRepo = GitRepo(templateID, "")
    val repos = TextMap("first50.txt", templateID)
    val cloned = SolrMap("GitRepos", templateRepo)
    val commitInfo = SolrMap("CommitInfo", GitCommitInfo(templateRepo))
    val whatsNext = SolrMap("Features", GitFeatures(templateRepo, feature.feature.Features()))
    val pipe = repos :--Clone("AkkaSpreadOutTest.conf")--> cloned :--CommitExtraction("AkkaSpreadOutTest.conf")--> commitInfo :--FeatureExtraction("AkkaSpreadOutTest.conf")--> whatsNext
    pipe.run("akka")
  }
}