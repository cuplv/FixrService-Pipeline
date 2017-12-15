import java.beans.XMLDecoder
import java.io.File

import bigglue.computations.Mapper
import bigglue.configurations.PipeConfig
import bigglue.data.{I, Identifiable, Identity}
import bigglue.store.instances.InMemDataMap
import bigglue.store.instances.file.FileSystemDataMap
import com.typesafe.config.Config
import spray.json.{JsArray, JsObject, JsString}

import scala.sys.process._
import scala.xml.XML


/**
  * Created by chanceroberts on 11/14/17.
  */

case class ACDFG() extends Identifiable[ACDFG]{
  override def mkIdentity(): Identity[ACDFG] = ???
}

case class FileLocation(subdir: String) extends Identifiable[FileLocation]{
  override def mkIdentity(): Identity[FileLocation] = ???
}

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

case class GraphExtraction(config: Config) extends Mapper[GitCommit, ACDFG](
  input => {
    val acdfg = config.getObject("acdfg").toConfig
    val androidHome: String = acdfg.getString("ANDROID_HOME")

    def getJarPath = {
      val androidLocation = s"$androidHome/platforms"
      val jarPath = s"android-${???}/android.jar"
      val jPath = s"$androidLocation/$jarPath"
      new FileSystemDataMap[I[String], I[String]](androidLocation).get(I(jarPath)) match {
        case None => throw new Exception(s"Cannot find the jar $jPath")
        case _ => jPath
      }
    }

    def getClassPath(repoLocation: String): String = {
      val repoMap = new FileSystemDataMap[I[String], I[String]](repoLocation)
      val manifestLists = repoMap.get(I("")) match{
        case Some(I(i)) => i.split("\n").foldRight(List[String]()) {
          case (str, lis) if str.contains("AndroidManifest.xml") =>
            s"$repoLocation/$str" :: lis
          case (_, lis) => lis
        }
        case _ => List[String]()
      }
      manifestLists.foldRight(""){
        case (pathOne, str) =>
          val xml = XML.loadFile(pathOne)
          val manifest = xml \\ "manifest"
          val pack = if ((manifest \ "@package" ).nonEmpty){
            (manifest \ "@package").toString
          }.replace(".", "/")
          println(pack)

          //val version = xml \\ "manifest" \ "@version"
          ???
      }
      println(manifestLists)
      ???
    }

    val subdir: String = acdfg.getString("extractorRepos")
    val repoLocation = s"$subdir/${input.gitRepo}"
    val extractorJar = acdfg.getString("extractorJar")
    val jre = acdfg.getString("jre")
    val user = input.gitRepo.substring(0, input.gitRepo.indexOf('/'))
    val repo = input.gitRepo.substring(input.gitRepo.indexOf('/')+1)
    val minHeapSize = "1024m"
    val maxHeapSize = "4096m"
    //val classPath = getClassPath(repoLocation)
    val graphPath = s"graph/${input.gitRepo}"
    //val classPath = getClassPath(repoLocation)
    // s"runlim --time-limit=1200 "
    println(s"java -Xms$minHeapSize -Xmx$maxHeapSize -jar $extractorJar -l ")
    /*
    println(s"java -Xms$minHeapSize -Xmx$maxHeapSize -jar $extractorJar -s false " +
      s"-o $graphPath -j true -z $subdir/${input.gitRepo} -t 60 -n " +
      // s"-l $classPath -o $graphPath -d $provenancePath -j true -z $subdir/${input.gitRepo} -t 60 -n " +
      s"$user -r $repo -h ${input.hash} -u https://github.com/${input.gitRepo}")
    ???
    s"java -Xms$minHeapSize -Xmx$maxHeapSize -jar $extractorJar -s false " +
      s"-o $graphPath -j true -z $subdir/${input.gitRepo} -t 60 -n " +
      //s"-l $classPath -o $graphPath -d $provenancePath -j true -z $subdir/${input.gitRepo} -t 60 -n " +
      s"$user -r $repo -h ${input.hash} -u https://github.com/${input.gitRepo}".!
    */
    ???
  }
)

object ACDFGPipeline {
  def main(args: Array[String]): Unit = {
    import bigglue.pipes.Implicits._
    val config = PipeConfig.newConfig()
    val tsConf = config.typeSafeConfig
    val toBeBuilt = new InMemDataMap[GitCommit, GitCommit]()
    val isBuilt = new InMemDataMap[GitCommit, GitCommit]()
    val isExtracted = new InMemDataMap[ACDFG, ACDFG]()
    val pipe = toBeBuilt :--BuildFiles(tsConf)-->isBuilt // :--GraphExtraction(tsConf)--> isExtracted
    //
    pipe.check(config)
    pipe.init(config)
    toBeBuilt.put(GitCommit("0legg/BezierClock", "a067ec6207246aaf8be94a7cdb59c9adb5c80b23"),
      GitCommit("0legg/BezierClock", "a067ec6207246aaf8be94a7cdb59c9adb5c80b23"))
    Thread.sleep(10000)
  }
}
