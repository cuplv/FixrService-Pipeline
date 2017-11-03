/**
  * Created by chanceroberts on 10/17/17.
  */
import spray.json.{JsNumber, JsObject, JsString, JsTrue}

import scalaj.http.{Http, HttpOptions}

object TestStuff {
  def main(args: Array[String]): Unit = {
    println(Http("http://localhost:8080/getCommits").timeout(1000, 3600000).postData(JsObject(Map("repo" -> JsString("42cc/p2psafety"),
    "sinceLast" -> JsTrue)).prettyPrint)
      .header("Content-Type", "application/json").option(HttpOptions.followRedirects(true)).asString.body)
  }
}
