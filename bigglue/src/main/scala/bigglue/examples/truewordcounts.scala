package bigglue.examples

import bigglue.computations.{Mapper, Reducer}
import bigglue.configurations.PipeConfig
import bigglue.data.serializers.JsonSerializer
import bigglue.data.{BasicIdentity, Identifiable, Identity}
import bigglue.store.instances.solr.SolrDataMap
import spray.json._

/**
  * Created by chanceroberts on 5/14/18.
  */

case class Word(num: Int, word: String) extends Identifiable[Word]{
  override def mkIdentity(): Identity[Word] = BasicIdentity(s"$num$word")
}

case object WordProtocols extends DefaultJsonProtocol{
  implicit val cSerial: JsonFormat[Count] = jsonFormat2(Count)
  implicit val wSerial: JsonFormat[Word] = jsonFormat2(Word)
}

case class WordSerializer() extends JsonSerializer[Word]{
  import WordProtocols.wSerial
  override def serializeToJson_(d: Word): JsObject = d.toJson.asJsObject
  override def deserialize_(json: JsObject): Word = json.convertTo[Word]
}

case class CountSerializer() extends JsonSerializer[Count]{
  import WordProtocols.cSerial
  override def serializeToJson_(d: Count): JsObject = d.toJson.asJsObject
  override def deserialize_(json: JsObject): Count = json.convertTo[Count]
}

case object IDFunc extends Mapper[Word, Word](a => List(a))

case object ChangeOne extends Mapper[Word, Word](a => List(Word(a.num, s"${a.word}_"))){
  override val versionOpt: Option[String] = Some("Underscores")
}

case object ChangeTwo extends Mapper[Word, Word](a => List()){
  override val versionOpt: Option[String] = Some("Duplicate")
}

case object ChangeThree extends Mapper[Word, Word](a => a.word match{
  case "get" => List(Word(a.num, "put"))
  case "put" => List(Word(a.num, "get"))
  case _ => List(a)
}){
  override val versionOpt: Option[String] = Some("SomeSwapped")
}

case object WordCount extends Reducer[Word, Count](i => BasicIdentity(i.word),
  i => o => Count(i.word, o.count+1),
  Count("", 0))

object truewordcounts {
  def main(args: Array[String]): Unit = {
    def addToDataMap(datMap: SolrDataMap[Word, Word], listOfWords: Array[String], numberToAdd: Int = 0): Unit = numberToAdd match{
      case 200000 => ()
      case x =>
        datMap.put_(Seq(Word(numberToAdd, listOfWords(numberToAdd%10))))
        addToDataMap(datMap, listOfWords, numberToAdd+1)
    }
    import bigglue.pipes.Implicits._
    val config = PipeConfig.newConfig()
    val startData = new SolrDataMap[Word, Word](WordSerializer(), "a")
    addToDataMap(startData, List("word", "again", "cat", "rat", "this", "hello", "a", "the", "ready", "glue",
                                 "mine", "yours", "end", "there", "put", "get", "new", "old", "dog", "orange").toArray)
    val secondData = new SolrDataMap[Word, Word](WordSerializer(), "b")
    val thirdData = new SolrDataMap[Word, Word](WordSerializer(), "c")
    val fourData = new SolrDataMap[Word, Word](WordSerializer(), "d")
    val finalData = new SolrDataMap[Count, Count](CountSerializer(), "e")
    val pipe = startData:--IDFunc-->secondData:--IDFunc-->thirdData:--IDFunc-->fourData:-+WordCount+->finalData
    pipe.check(config)
    pipe.init(config)
    pipe.persist()
    Thread.sleep(20000)
    println(finalData.all())
  }
}
