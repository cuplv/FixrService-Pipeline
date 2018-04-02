package bigglue.examples

import bigglue.computations.Mapper
import bigglue.configurations.PipeConfig
import bigglue.data.I
import bigglue.data.serializers.JsonSerializer
import bigglue.store.instances.solr.SolrDataMap
import spray.json._

/**
  * Created by chanceroberts on 4/2/18.
  */

case object A extends Mapper[I[Int], I[Int]](input => { List(I(input.a+2))}){
}

case object B extends Mapper[I[Int], I[Int]](input => { List(I(input.a*3))}){
  override val versionOpt = Some("0.1")
}

case object IntProtocols extends DefaultJsonProtocol{
  implicit val iSerial = jsonFormat1(I[Int])
}

case class IIntSerializer() extends JsonSerializer[I[Int]]{
  import IntProtocols.iSerial
  override def serializeToJson_(d: I[Int]): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): I[Int] = json.convertTo[I[Int]]
}

object simpleExample {
  def main(args: Array[String]): Unit ={
    val conf = PipeConfig.newConfig()
    import bigglue.pipes.Implicits._
    val a = new SolrDataMap[I[Int], I[Int]](IIntSerializer(), "a")
    val b = new SolrDataMap[I[Int], I[Int]](IIntSerializer(), "b")
    val c = new SolrDataMap[I[Int], I[Int]](IIntSerializer(), "c")
    val pipe = a:--A-->b:--B-->c
    pipe.check(conf)
    pipe.init(conf)
    pipe.run()
  }
}
