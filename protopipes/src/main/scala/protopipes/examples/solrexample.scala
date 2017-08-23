package protopipes.examples

import protopipes.data.{BasicIdentity, I, Identifiable, Identity}
import protopipes.data.serializers.JsonSerializer
import spray.json._

/**
  * Created by chanceroberts on 8/23/17.
  */
case class SolrTest(genericList: List[Int] = List(1,2,3), genericString: String = "Hey", genericNumber: Int = 10) extends Identifiable[SolrTest]{
  override def mkIdentity(): Identity[SolrTest] = BasicIdentity("?!?!?")
}

case class SolrTest2(solrtest: SolrTest)

object SolrProtocol extends DefaultJsonProtocol {
  implicit val solr: JsonFormat[SolrTest] = jsonFormat3(SolrTest)
  implicit val solr2: JsonFormat[SolrTest2] = jsonFormat1(SolrTest2)
}

object SerializeSolrTest extends JsonSerializer[SolrTest] {
  import SolrProtocol._
  override def serializeToJson_(d: SolrTest): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): SolrTest = json.convertTo[SolrTest]
}


object solrexample {
  def main(args: Array[String]): Unit = {
    val serializer = SerializeSolrTest
    println(SolrTest())
    println(serializer.serialize_(SolrTest()))
    println(serializer.deserialize_(serializer.serialize_(SolrTest())))
  }
}
