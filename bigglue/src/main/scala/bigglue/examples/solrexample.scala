package bigglue.examples

import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.data.serializers.JsonSerializer
import bigglue.store.instances.solr.{SolrDataMap, SolrMultiMap}
import spray.json._


/**
  * Created by chanceroberts on 8/23/17.
  */
case class SolrTest(genericList: List[Int] = List(1,2,3), genericString: String = "Hey", genericNumber: Int = 10) extends Identifiable[SolrTest]{
  override def mkIdentity(): Identity[SolrTest] = BasicIdentity("?!?!?")
}

case class SolrTest2(solrtest: Option[SolrTest]) extends Identifiable[SolrTest2]{
  override def mkIdentity(): Identity[SolrTest2] = BasicIdentity("?!?!?")
}

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
  val serializer = SerializeSolrTest
  def testSolrMap(): Unit = {
    val sDataMap = new SolrDataMap[String, SolrTest](serializer, "test")
    sDataMap.put("testOne", SolrTest())
    sDataMap.put("testTwo", SolrTest(List(), ""))
    println(sDataMap.get("testOne"))
    println(sDataMap.get("testTwo"))
    println(sDataMap.all())
    println(sDataMap.size())
    println(sDataMap.extract())
    println(sDataMap.get("testOne"))
    sDataMap.put("testThree", SolrTest(List(1,1,1,1,1,1), "SolrTest"))
    println(sDataMap.get("testThree"))
    sDataMap.remove("testThree")
    println(sDataMap.get("testThree"))
  }

  def testSolrMultiMap(): Unit = {
    val sMultiMap = new SolrMultiMap[String, SolrTest](serializer, "test")
    sMultiMap.put("testOne", Set(SolrTest(), SolrTest(List(2,3,4,5)), SolrTest(List(1,2,3), "Hey", 12)))
    sMultiMap.put("testTwo", Set(SolrTest(), SolrTest(List(), "", 0)))
    println(sMultiMap.get("testOne"))
    println(sMultiMap.get("testTwo"))
    println(sMultiMap.all())
    println(sMultiMap.size())
    sMultiMap.extract()
    sMultiMap.put("testThree", Set(SolrTest(List(1,2,3,4,5)), SolrTest(List()), SolrTest(), SolrTest(List(5,5,5))))
    println(sMultiMap.get("testThree"))
    sMultiMap.remove("testThree")
    println(sMultiMap.get("testThree"))
  }

  def testSolrIterator(): Unit = {
    def addMoreObjects(sDataMap: SolrDataMap[String, SolrTest], objectsLeft: Int): Unit = objectsLeft match {
      case 0 => ()
      case x =>
        sDataMap.put(objectsLeft.toString, SolrTest(List(1,2,3), "Hey", objectsLeft))
        addMoreObjects(sDataMap, objectsLeft-1)
    }
    def extractFromIterator(iterator: Iterator[SolrTest]): Unit = if (iterator.hasNext){
      println(iterator.next())
      extractFromIterator(iterator)
    } else ()
    val sDataMap = new SolrDataMap[String, SolrTest](serializer, "test")
    addMoreObjects(sDataMap, 1000)
    val solrIterator = sDataMap.iterator()
    extractFromIterator(solrIterator)
  }

  def main(args: Array[String]): Unit = {
    //println(Http("http://localhost:8983/solr/notACore/select?q=\"*:*\"&wt=json").asString.body.parseJson)
    testSolrMap()
  }
}
