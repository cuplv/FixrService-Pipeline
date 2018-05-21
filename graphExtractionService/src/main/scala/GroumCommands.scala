import com.typesafe.config.Config
import spray.json._

import scala.sys.process._
import scala.util.Random

/**
  * Created by chanceroberts on 5/17/18.
  */
object GroumCommands {
  def runCommand(string: String, stdIn: String = ""): Unit = {
    if (stdIn.length() == 0){
      // println(string)
      string.!
    } else {
      // println(s"echo $stdIn | $string")
      (s"echo $stdIn" #| string).!
    }
  }

  def setupDocker(conf: Config): Unit = {
    try {
      s"docker --version".!!
    } catch{
      case e: Exception =>
        throw new Exception("A version of Docker needs to be set up before the service is able to run.")
    }
    try {
      println(s"Starting up container ${conf.getString("bgContainer")}")
      s"docker run -dit --name biggroum ${conf.getString("bgContainer")}".!!
    } catch{
      case e: Exception => try{
        s"docker stop biggroum".!
        s"docker rm biggroum".!
        s"docker run -dit --name biggroum ${conf.getString("bgContainer")}".!!
      }
      catch{
        case e: Exception => throw new Exception(s"application.conf's bgContainer " +
          s"(${conf.getString("bgContainer")}) does not point to a Docker Container")
      }
    }
    s"docker exec -i biggroum mkdir apk".!
    println(s"Container ${conf.getString("bgContainer")} is ready to go!")
  }

  def removeDocker(): Unit = {
    println(s"Stopping ${s"docker container stop biggroum".!!.split('\n')(0)}.")
    println(s"Removing Container ${s"docker rm biggroum".!!.split('\n')(0)}.")
  }

  def extractGraph(queryStr: String, config: Config): String = {
    val results = try {
      val apkRV = Random.nextInt()
      println(apkRV)
      val apkName = s"app-$apkRV.apk"
      val json = queryStr.parseJson.asJsObject
      val normalApk = json.fields.get("apk") match {
        case Some(JsString(j)) => j
        case None => throw new Exception("Value apk must exist. (Example: {apk: ../myapk.apk})")
        case _ => throw new Exception("apk path must be a string.")
      }
      val outputFolder = s"apk/$apkRV-output"
      s"docker exec -i biggroum mkdir $outputFolder".!
      s"docker cp $normalApk biggroum:home/biggroum/apk/$apkName".!
      val command = s"docker exec -i -w ${config.getString("extractorLocation")} biggroum " +
        s"java -Xms1024m -Xmx4096m -jar fixrgraphextractor_2.12-0.1.0-one-jar.jar -j false -z tmpFolder -t 100"
      val userComm = json.fields.get("user") match {
        case Some(JsString(j)) => s"$command -n $j"
        case None => throw new Exception("Value user must exist. (Example: {user: MyUser})")
        case _ => throw new Exception("The user must be a string.")
      }
      val repoComm = json.fields.get("repo") match{
        case Some(JsString(j)) => s"$userComm -r $j"
        case None => userComm
        case _ => throw new Exception("The repo must be a string.")
      }
      val hashComm = json.fields.get("hash") match {
        case Some(JsString(j)) => s"$repoComm -h $j"
        case None => repoComm
        case _ => throw new Exception("The hash must be a string.")
      }
      val trueComm = s"$hashComm -o ../../../../$outputFolder -a true -p ../../../../apk/$apkName " +
        "-l /usr/lib/jvm/java-8-oracle/jre/lib/rt.jar -w ../../../../android-sdk-linux/platforms"
      trueComm.!
      //Now, there should be something in the output folder specified!
      val values = s"docker exec -i biggroum ls $outputFolder".!!.split('\n').toList
      val graphs = values.foldRight(List[JsValue]()){
        case (str, lis) =>
          println(s"Getting contents of $str!")
          val proto = s"docker exec -i biggroum cat $outputFolder/$str".!!
          JsObject("file" -> JsString(str), "contents" -> JsString(proto)) :: lis
      }
      println("Done with the extractor!")
      s"docker exec -i biggroum rm -f $apkName".!
      s"docker exec -i biggroum rm -rf $outputFolder".!
      JsObject("status" -> JsString("ok"), "results" -> JsArray(graphs.toVector))
    } catch{
      case e: Exception =>
        JsObject("status" -> JsString("error"), "results" -> JsString(e.getMessage))
    }
    val toReturn = results.prettyPrint
    println("Ready to send back!")
    toReturn
  }
}
