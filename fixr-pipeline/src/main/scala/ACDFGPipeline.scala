import java.beans.XMLDecoder
import java.io.File

import bigglue.computations.Mapper
import bigglue.configurations.PipeConfig
import bigglue.data.serializers.JsonSerializer
import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.pipes.Pipe
import bigglue.store.instances.InMemDataMap
import bigglue.store.instances.file.FileSystemDataMap
import bigglue.store.instances.solr.SolrDataMap
import com.typesafe.config.Config
import spray.json._

import scala.sys.process._
import scala.xml.XML
import scalaj.http.{Http, HttpOptions}


/**
  * Created by chanceroberts on 11/14/17.
  */

case class APK(user: String, repo: String, hash: String, file: String) extends Identifiable[APK]{
  override def mkIdentity(): Identity[APK] = BasicIdentity(s"$user/$repo:$hash")
}

case object ACDFGProtocols extends DefaultJsonProtocol{
  implicit val apkProt = jsonFormat4(APK)
  implicit val acdfgProt = jsonFormat2(ACDFG)
}

case class APKSerializer() extends JsonSerializer[APK]{
  import ACDFGProtocols.apkProt
  override def serializeToJson_(d: APK): JsObject = d.toJson.asJsObject
  override def deserialize_(json: JsObject): APK = json.convertTo[APK]
}

case class ACDFG(location: String, contents: String) extends Identifiable[ACDFG]{
  override def mkIdentity(): Identity[ACDFG] = BasicIdentity(location)
}

case class ACDFGSerializer() extends JsonSerializer[ACDFG]{
  import ACDFGProtocols.acdfgProt
  override def serializeToJson_(d: ACDFG): JsObject = d.toJson.asJsObject
  override def deserialize_(json: JsObject): ACDFG = json.convertTo[ACDFG]
}

case class GraphExtraction(config: Config) extends Mapper[APK, ACDFG](
  apk => {
    println("Running Extractor")
    println(config.getObject("acdfg"))
    val serviceLocation = config.getObject("acdfg").toConfig.getString("serviceLocation")
    println(s"$serviceLocation/extract")
    val ml = Http(s"$serviceLocation/extract").timeout(1000, 3600000).postData(
      JsObject("apk" -> JsString(apk.file), "user" -> JsString(apk.user), "repo" -> JsString(apk.repo),
        "hash" -> JsString(apk.hash)).prettyPrint).header("Content-Type", "application/json")
      .option(HttpOptions.followRedirects(true)).asString.body.parseJson.asJsObject
    ml.fields.get("status") match{
      case Some(JsString("ok")) => ()
      case Some(_) => throw new Exception("FixrGraphExtractor ran into an error: " + ml.fields("results").prettyPrint)
      case _ => throw new Exception("FixrGraphExtractor not returning correct values. " +
        "Make sure you're running the correct service?")
    }
    ml.fields.get("results") match{
      case Some(JsArray(a)) => a.foldRight(List[ACDFG]()){
        case (j: JsObject, acdfgs) =>
          (j.fields.get("file"), j.fields.get("contents")) match{
            case (Some(JsString(file)), Some(JsString(contents))) => ACDFG(file, contents) :: acdfgs
            case _ => acdfgs
          }
        case (_, acdfgs) => acdfgs
      }
      case _ => throw new Exception("This did not extract properly... Expected an array of values. :(")
    }
  }
)

/*case class FileLocation(subdir: String) extends Identifiable[FileLocation]{
  override def mkIdentity(): Identity[FileLocation] =
}*/

/*
object CopyFiles extends Mapper[GitCommit, GitCommit](
  input => {
    // Copy
    val fileSystemMap = new FileSystemDataMap[I[String], I[String]]("commitGraphs")
    val gitRepo = input.gitRepo
    val uSplit = gitRepo.charAt('/')
    val toGraph = JsArray(JsObject(Map("user" -> JsString(gitRepo.substring(0, uSplit)),
      "repo" -> JsString(gitRepo.substring(uSplit+1)), "hash" -> JsString(input.hash))))
    fileSystemMap.put(I(s"${input.gitRepo}/${input.hash}.json"), I(toGraph.prettyPrint))


    ???
  }
)

case class BuildFiles(config: Config) extends Mapper[GitCommit, GitCommit](
  input => {
    // Build it
    val subdir: String = config.getObject("acdfg").toConfig.getString("extractorRepos")
    val repoLocation = s"$subdir/${input.gitRepo}"
    val repoMap = new FileSystemDataMap[I[String], I[String]](repoLocation)
    val file = new File(repoLocation)

    val buildLocation = repoMap.get(I("")) match{
      case Some(I(str)) => str.split("\n").filter(input => input.contains("build.gradle")).toList match{
        case Nil => throw new Exception("This file is not buildable.")
        case gradle :: lis =>
          gradle.length match{
            case 12 => "/"
            case x => s"/${gradle.substring(0,x-12)}"
          }
      }
      case _ => throw new Exception("This file is not buildable.")
    }
    val gradlew = s"${buildLocation}gradlew"
    println(gradlew)
    val compile_cmd = repoMap.get(I(gradlew)) match{
      case None => "gradle"
      case Some(gradleLoc) =>
        //Do some chmod stuff here?
        s"./$repoLocation$gradlew"
    }
    s"$compile_cmd --stacktrace -p $repoLocation$buildLocation".!
    List(input)
  }
)
*/

object ACDFGPipeline {
  def main(args: Array[String]): Unit = {
    import bigglue.pipes.Implicits._
    val config = PipeConfig.newConfig()
    val tsConf = config.typeSafeConfig
    /*val toBeBuilt = new InMemDataMap[GitCommit, GitCommit]()
    val isBuilt = new InMemDataMap[GitCommit, GitCommit]()
    val isExtracted = new InMemDataMap[ACDFG, ACDFG]()*/
    val apks = new SolrDataMap[APK, APK](APKSerializer(), "APKs")
    apks.put_(List(APK("jcarolus", "android-chess",
      "4d83409b4c66e0ee750433f9902f591692a302b3", "../../buildableRepos/android-chess/app/build/outputs/apk/app-release-unsigned.apk")))
    val graphs = new SolrDataMap[ACDFG, ACDFG](ACDFGSerializer(), "ACDFGs")
    val pipe = apks:--GraphExtraction(tsConf)-->graphs
    pipe.check(config)
    pipe.init(config)
    pipe.persist()
    // pipe.check(config)
    // pipe.init(config)
    /*toBeBuilt.put_(GitCommit("0legg/BezierClock", "a067ec6207246aaf8be94a7cdb59c9adb5c80b23"),
      GitCommit("0legg/BezierClock", "a067ec6207246aaf8be94a7cdb59c9adb5c80b23"))*/
    Thread.sleep(10000)
  }
}

