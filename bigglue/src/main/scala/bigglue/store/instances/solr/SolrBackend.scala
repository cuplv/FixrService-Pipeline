package bigglue.store.instances.solr

import java.io.{BufferedWriter, File, FileWriter}

import com.typesafe.config.{Config, ConfigFactory}
import bigglue.configurations.{ConfOpt, Constant, PipeConfig}
import bigglue.data.Identifiable
import bigglue.exceptions.{NotInitializedException, ProtoPipeException}
import spray.json._

import scala.collection.JavaConverters._
import scala.io.Source
import scalaj.http.Http

/**
  * Created by chanceroberts on 8/25/17.
  */
class SolrBackend[Data <: Identifiable[Data]](nam: String, config: Config){
  val solrFallbackConf: Config = ConfigFactory.parseMap(
    Map[String, Any]("solrLocation" -> "localhost:8983", "solrServerLocation" -> "~/solr/server",
      "solrCoreInformation" -> "~/solr/server/solr/configsets/data_driven_schema_configs/conf", "isCloud" -> false,
      "hasSchema" -> false).asJava)
  val solrNormalConfig: Config = try{
    config.getConfig(Constant.BIGGLUE).getConfig("_solr")
  } catch{
    case e: Exception => ConfigFactory.parseMap(Map[String, Any]().asJava)
  }
  val solrConfig: Config = PipeConfig.resolveOptions(solrFallbackConf, ConfOpt.typesafeConfig(solrNormalConfig))
  val solrLocation: String = solrConfig.getString("solrLocation")
  val solrServerLocation: String = solrConfig.getString("solrServerLocation")
  val url = s"http://$solrLocation/solr/$nam/"
  var fieldsAdded: Map[String, Unit] = Map()
  var templateOpt: Option[JsObject] = None
  var toCommit: Boolean = false
  val hasSchema: Boolean = solrConfig.getBoolean("hasSchema")
  try {
    //Make sure the core doesn't actually exist first.
    Http(url + "select?wt=json&q=\"*:*\"").asString.body.parseJson
  } catch {
    case e: Exception =>
      createCore()
      //throw new NotInitializedException(s"Core $nam", s"Creating a Data Store on core $nam.", None)
  }

  def keyToString(key: Any): String = {
    key match {
      case i: Identifiable[_] => i.getId().toString
      case _ => key.toString
    }
  }

  def getDocumentsFromQuery(query: String): List[JsObject] = {
    val queryURL = url+query
    val json = Http(queryURL).asString.body
    json.parseJson.asJsObject().fields.get("response") match {
      case Some(response: JsValue) => response.asJsObject.fields.get("docs") match {
        case Some(docs: JsArray) =>
          docs.elements.toList.map{
            case doc if hasSchema => doc.asJsObject
            case doc => mkDocDeserializable(doc.asJsObject)
          }
        case _ =>
          throw new ProtoPipeException(Some(s"SolrMap $nam response is malformed. Are you sure you're really using Solr?"), None)
      }
      case _ =>
        throw new ProtoPipeException(Some(s"Probably got an error in SolrMap $nam on query $query. Please fix your query."), None)
    }
  }

  def getDocument(id: String): Option[JsObject] = {
    if (toCommit){
      println(url+"update")
      Http(url+"update").postData("{ \"commit\": {} }".getBytes).header("Content-Type", "application/json").asString.body
      toCommit = false
    }
    def getDocumentFromList(list: List[JsObject], id: String): Option[JsObject] = list match{
      case Nil => None
      case first :: rest => first.fields.get("id") match{
        case Some(x: JsString) if x.value.equals(id) => Some(first)
        case _ => getDocumentFromList(rest, id)
      }
    }
    val docsToCheck = getDocumentsFromQuery("select?wt=json&q=id:\"" + id + "\"")
    getDocumentFromList(docsToCheck, id)
  }

  def getDocuments(id: String, field: String = "_id_"): List[JsObject] = {
    if (toCommit){
      Http(url+"update").postData("{ \"commit\": {} }".getBytes).header("Content-Type", "application/json").asString.body
      toCommit = false
    }
    val docsToCheck = getDocumentsFromQuery("select?wt=json&rows=1000000000&q="+field+":\""+ id + "\"")
    docsToCheck.foldRight(List[JsObject]()) {
      case (v: JsValue, list) =>
        val obj = v.asJsObject()
        obj.fields.get(field) match {
          case Some(str: JsString) if str.value.equals(id) => obj :: list
          case _ => list
        }
    }
  }

  def addDocument(doc: JsObject, key: String): Unit = {
    val newDoc = if (hasSchema) doc else saveTheOmitted(doc)
    val json = JsObject(Map("add" -> JsObject(Map("doc" -> newDoc, "commitWithin" -> JsNumber(1000))))).toString()
    toCommit = true
    //JsObject(Map("add" -> JsObject(Map("doc" -> newDoc)), "commit" -> JsObject())).toString()
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    result.parseJson.asJsObject.fields.get("error") match{
      case Some(x) => throw new Exception(s"Could not update the core $nam on key $key.")
      case _ => ()
    }
    templateOpt match{
      case None => templateOpt = Some(doc)
      case _ => ()
    }
  }

  def deleteDocuments(ids: List[String]): Unit = {
    val json = JsObject(Map("delete" -> JsArray(ids.map(JsString(_)).toVector))).toString() //, "commit" -> JsObject())).toString
    toCommit = true
    val result = Http(url+"update").postData(json.getBytes).header("Content-Type", "application/json").asString.body
    result.parseJson.asJsObject.fields.get("error") match{
      case Some(x) => throw new Exception(s"Could not update the core $nam on ID(s) $ids.")
      case _ => ()
    }
  }

  def getAllDocuments: List[JsObject] = getDocumentsFromQuery("select?wt=json&rows=100000000&q=*:*")

  private def mkDocDeserializable(doc: JsObject): JsObject = templateOpt match {
    case None => doc
    case Some(template) =>
      JsObject(doc.fields.foldRight(Map[String, JsValue]()){
        case ((key, jsValue), fieldMap) => key match{
          case "_version_" => fieldMap
          case "_EmptyFields" => jsValue match{
            case x: JsArray => x.elements.foldRight(fieldMap){
              case (s: JsString, fMap) =>
                fMap + (s.value->JsArray(Vector()))
              case (_, fMap) => fMap
            }
            case _ => fieldMap + ("_EmptyFields" -> jsValue)
          }
          case _ => (jsValue, template.fields.get(key)) match {
            case (trueValue: JsArray, Some(_: JsArray)) =>
              fieldMap + (key -> JsArray(trueValue.elements.map{
                case JsString("_ _") => JsString("")
                case x => x
              }))
            case (trueValue, Some(_: JsArray)) => fieldMap + (key -> JsArray(trueValue))
            case (JsArray(Vector(JsString("_ _"))), Some(_)) => fieldMap + (key -> JsString(""))
            case (trueValue: JsArray, Some(_)) => fieldMap + (key -> trueValue.elements.head)
            case (JsString("_ _"), _) => fieldMap + (key -> JsString(""))
            case (trueValue, _) => fieldMap + (key -> trueValue)
          }
        }
      })
  }


  private def saveTheOmitted(doc: JsObject): JsObject = {
    val newDocFields = doc.fields.foldRight((Map[String, JsValue](), List[JsString]())){
      case ((str, jsValue), (mp, missingFields)) =>
        jsValue match{
        case JsArray(Vector()) => (mp, JsString(str) :: missingFields)
        case JsString("") => (mp + (str->JsString("_ _")), missingFields)
        case j: JsArray => (mp + (str->JsArray(j.elements.foldRight(List[JsValue]()){
          case (jsV, lis) => (jsV match{
            case JsString("") => JsString("_ _")
            case x => x
          }) :: lis
        }.toVector)), missingFields)
        case _ => (mp + (str->jsValue), missingFields)
      }
    }
    JsObject(newDocFields._1 + ("_EmptyFields" -> JsArray(newDocFields._2.toVector)))
  }

  private def mkJsObjectsSolrable(doc: JsObject): JsObject = {
    val newDocFields = doc.fields.foldRight(Map[String, JsValue]()) {
      case ((str, jsValue), newDoc) => jsValue match {
        case j: JsObject => newDoc + (str -> JsString(j.toString))
        case _ => newDoc + (str -> jsValue)
      }
    }
    JsObject(newDocFields)
  }

  private def returnNestedJsObjects(doc: JsObject): JsObject = {
    JsObject(doc.fields.foldRight(Map[String, JsValue]()){
      case ((str, jsValue), newDoc) => jsValue match{
        case j: JsString if j.value.length() > 1 && j.value.charAt(0) == '{' =>
          try{
            newDoc+(str->j.value.parseJson.asJsObject)
          } catch{
            case e: Exception => newDoc+(str->jsValue)
          }
        case _ => newDoc+(str->jsValue)
      }
    })
  }

  private def createCore(): Unit = {
    def copyAFile(src: String, dest: String) = {
      val destFile = new File(dest)
      destFile.createNewFile()
      val destWriter = new BufferedWriter(new FileWriter(destFile))
      val srcReader = Source.fromFile(src)
      srcReader.getLines().foreach(line => destWriter.write(line))
      srcReader.close()
      destWriter.close()
    }
    if (solrConfig.getBoolean("isCloud")) {
      Http(s"http://$solrLocation/solr/admin/collections?action=CREATE&name=$nam&numShards=1").asString.body
    } else {
      val newCore = new File(s"$solrServerLocation/solr/$nam")
      if (!newCore.isDirectory) newCore.mkdir()
      val confInfo = new File(s"$solrServerLocation/solr/$nam/conf")
      if (!confInfo.isDirectory) confInfo.mkdir()
      val solrInformation = new File(solrConfig.getString("solrCoreInformation"))
      def recursivelyCopy(newFile: File): Unit ={
        newFile.listFiles().foreach{
          file =>
            val dest = file.getPath.diff(solrInformation.getPath)
            if (file.isDirectory){
              new File(s"$solrServerLocation/solr/$nam/conf$dest").mkdir()
              recursivelyCopy(file)
            }
            else{
              copyAFile(file.getPath, s"$solrServerLocation/solr/$nam/conf$dest")
            }
        }
      }
      if (solrInformation.isDirectory) recursivelyCopy(solrInformation)
      else throw new ProtoPipeException(Some("solrCoreInformation was expected to be a directory."), None)
      Http(s"http://$solrLocation/solr/admin/cores?action=CREATE&name=$nam").asString.body
    }
  }
}

