import spray.json._
import scala.sys.process._
import scalaj.http.{Http, HttpOptions}
/**
  * Created by chanceroberts on 5/17/18.
  */
object TestGraphExtractionService {
  def splitIntoSeveral(string: String): Unit = string.length match{
    case x if x > 200 =>
      println(string.substring(0, 200))
      splitIntoSeveral(string.substring(200))
    case _ =>
      println(string)
      string
  }

  def main(args: Array[String]): Unit = {
    val testingAPK = "../../buildableRepos/android-chess/app/build/outputs/apk/app-release-unsigned.apk"
    /*val ml = Http("http://localhost:8010/end").timeout(1000, 3600000).postData(
      JsObject(Map("apk" -> JsString(testingAPK), "user" -> JsString("jcarolus"), "repo" -> JsString("android-chess"),
        "hash" -> JsString("4d83409b4c66e0ee750433f9902f591692a302b3"))).prettyPrint)
      .header("Content-Type", "application/json").option(HttpOptions.followRedirects(true)).asString.body*/
    val ml = Http("http://localhost:8010/extract").timeout(1000, 3600000).postData(
    JsObject(Map("apk" -> JsString(testingAPK), "user" -> JsString("jcarolus"), "repo" -> JsString("android-chess"),
      "hash" -> JsString("4d83409b4c66e0ee750433f9902f591692a302b3"))).prettyPrint)
    .header("Content-Type", "application/json").option(HttpOptions.followRedirects(true)).asString.body
    val obj = ml.parseJson.asJsObject
    assert(obj.fields.getOrElse("status", "error") match{
      case JsString(j) => j.equals("ok")
      case _ => false
    })
    obj.fields.get("results") match{
      case Some(JsArray(res)) => res.foreach(anotherObj => println(anotherObj.asJsObject.fields.getOrElse("file", JsString("")) match{
        case JsString(j) => j
        case _ => ""
      }))
      case _ => throw new Exception("Results didn't return correctly... :(")
    }
  }
}
