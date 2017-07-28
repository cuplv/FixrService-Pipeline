import akka.actor.ActorSystem
import pipecombi._
import com.typesafe.config.Config
import mthread_abstrac.ConfigHelper

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

case class SolrMap[SDoc <: Identifiable](name: String, conf: Config = null) extends DataMap[SDoc] {
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

  def getObject(k: String = ""): List[(String, Any)] = {
    val queryURL = url + "select?wt=json&q=key:" + k
    val json = Http(queryURL).asString.body //Query the Database Using the URL
    JSON.parseFull(json) match {
      case Some(parsed: Map[String@unchecked, Any@unchecked]) =>
        parsed.get("response") match {
          case Some(resp: Map[String@unchecked, Any@unchecked]) =>
            resp.get("docs") match {
              case Some((first: Map[String@unchecked, Any@unchecked]) :: list) =>
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
           |      "id": \"$id\",
           |      "$fName": \"$itemF\"
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
                      |      "$fName": $itemF,""".stripMargin, false)
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
                           |      """.stripMargin + "\"" + fName + "\": " + itemF + ","
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
    s.indexOf(delimiter) match{
      case -1 => new Identifiable{
        def identity() = Identity(s, None)
      }.asInstanceOf[SDoc]
      case x => new Identifiable{
        def identity() = Identity(s.substring(0,x), Some(s.substring(x+delimiter.length)))
      }.asInstanceOf[SDoc]
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
    val queryURL = url+"select?q=id:"+id+"&wt=json"
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
    val queryURL = url+"select?wt=json&rows=1000000&q=*:*"
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
                      case Some(List(v2)) => getIdentity(v.toString) :: l
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
    val queryURL = url+"select?wt=json&rows=1000000&q=*:*"
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

// Mock feature transformers

case class Clone() extends IncrTransformer[GitID, GitRepo] {
  override val version = "0.1"

  override val statMap = SolrMap[Stat]("StatMap")
  override val provMap = SolrMap[Identity]("ProvMap")
  override val errMap = SolrMap[ErrorSummary]("ErrorMap")

  override def compute(input: GitID): List[GitRepo] = ???
}

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

  val fixr = (gitIds :--Clone()--> gitRepos :--Build()--> gitBuilds) :<  {
    (CallbackInstr()--> instrAPKs :--Loop[InstrumentedAPKs]--> instrAPKs :--Stop[InstrumentedAPKs]--> end) ~
    (ExtractGroum()--> groums :--Loop[Groums]--> groums)
  }

  fixr

  /*
  || {
    (CallbackInstr--> instrAPKs :--Stop[InstrumentedAPKs]()--> end) ~
    (ExtractGroum--> groums :--Stop[Groums]()--> end)
  } */
}

object Test{
  def main(args: Array[String]): Unit = {
    val test = SolrMap[Identifiable]("status")
    test.put(Identity("a", None), Done)
    test.put(Identity("b", None), NotDone)
  }
}