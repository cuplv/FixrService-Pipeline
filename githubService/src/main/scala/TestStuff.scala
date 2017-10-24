/**
  * Created by chanceroberts on 10/17/17.
  */
import bigglue.data.I
import spray.json.{JsObject, JsString}

import scalaj.http.{Http, HttpOptions}
import bigglue.store.instances.file.FileSystemDataMap

object TestStuff {
  def main(args: Array[String]): Unit = {
    var fileSystem = new FileSystemDataMap[I[String], I[String]]("repos")
    println(Http("http://localhost:8080/patch").timeout(1000, 3600000).postData(JsObject(Map("repo" -> JsString("42cc/p2psafety"),
      "commit"->JsString("e1dfd39e85cab1ada706d3d465a001a71efc3016"),
      "file"->JsString("p2psafety-android/p2psafety/src/ua/p2psafety/SosActivity.java"))).prettyPrint)
      .header("Content-Type", "application/json").option(HttpOptions.followRedirects(true)).asString.body)
  }
}
