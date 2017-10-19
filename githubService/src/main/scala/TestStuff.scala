/**
  * Created by chanceroberts on 10/17/17.
  */
import bigglue.data.I
import spray.json.{JsObject, JsString, JsTrue}

import scalaj.http.{Http, HttpOptions}
import bigglue.store.instances.file.FileSystemDataMap

object TestStuff {
  def main(args: Array[String]): Unit = {
    var fileSystem = new FileSystemDataMap[I[String], I[String]]("repos")
    println(Http("http://localhost:8080/commitInformation").timeout(1000, 3600000).postData(JsObject(Map("repo" -> JsString("42cc/p2psafety"),
      "commit"->JsString("42cfd4bc8932d145492debaddf28bfb12b54d6c2"))).prettyPrint)
      .header("Content-Type", "application/json").option(HttpOptions.followRedirects(true)).asString.body)
  }
}
