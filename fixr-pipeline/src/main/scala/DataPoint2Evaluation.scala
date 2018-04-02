import bigglue.computations.Mapper
import bigglue.configurations.PipeConfig
import bigglue.data.I
import bigglue.data.serializers.JsonSerializer
import bigglue.store.instances.solr.SolrDataMap
import spray.json._

/**
  * Created by chanceroberts on 3/7/18.
  */

object MockProtocol1 extends DefaultJsonProtocol {
  implicit val iin: JsonFormat[I[Int]] = jsonFormat1(I[Int])
}

case class ExpensiveComputation(func: Int => Int) extends Mapper[I[Int], I[Int]](input => {
  Thread.sleep(2000)
  List(I(func(input.i)))
})

case class MistakeMade() extends Mapper[I[Int], I[Int]](input => {
  List(I(input.a))
}){
  override val versionOpt = Some("v1")
}

case object IIntSerializer extends JsonSerializer[I[Int]]{
  import MockProtocol1.iin
  override def serializeToJson_(d: I[Int]): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): I[Int] = json.convertTo[I[Int]]
}

object DataPoint2Evaluation {
  def main(args: Array[String]): Unit = {
    import bigglue.pipes.Implicits._
    val config = PipeConfig.newConfig()
    val start: SolrDataMap[I[Int], I[Int]] = new SolrDataMap(IIntSerializer, "start")
    val middle: SolrDataMap[I[Int], I[Int]] = new SolrDataMap(IIntSerializer, "middle")
    val end: SolrDataMap[I[Int], I[Int]] = new SolrDataMap(IIntSerializer, "end")
    val pipe = start:--ExpensiveComputation(i => i+3)-->middle:--MistakeMade()-->end
    pipe.check(config)
    pipe.init(config)
    def generateSequence(end: Int, front: Int = 0): List[I[Int]] = {
      if (front == end) List(I(front))
      else I(front) :: generateSequence(end, front+1)
    }
    start.put(generateSequence(50))
  }
}
