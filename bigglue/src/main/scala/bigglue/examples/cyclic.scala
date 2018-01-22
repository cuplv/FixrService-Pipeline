package bigglue.examples

import bigglue.computations.Mapper
import bigglue.configurations.{DataStoreBuilder, PipeConfig}
import bigglue.data.serializers.JsonSerializer
import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.store.instances.InMemIdDataMap
import bigglue.store.instances.solr.SolrDataMap
import spray.json._

/**
  * Created by chanceroberts on 12/14/17.
  */
case class Cycleable(time: Int) extends Identifiable[Cycleable]{
  override def mkIdentity(): Identity[Cycleable] = BasicIdentity("Same")
}

object MockProtocol extends DefaultJsonProtocol{
  implicit val cycle: JsonFormat[Cycleable] = jsonFormat1(Cycleable)
}

case class CycleableSerializer() extends JsonSerializer[Cycleable]{
  import MockProtocol._

  override def serializeToJson_(d: Cycleable): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): Cycleable = json.convertTo[Cycleable]
}

case class M1() extends Mapper[Cycleable, Cycleable](input => {
  println(s"Cycle ${input.time}!")
  List(input)
})

case class M2() extends Mapper[Cycleable, I[(Cycleable, I[Int])]](input => {
  Thread.sleep(1000)
  List(I((Cycleable(input.time), I(input.time))))
})


object cyclic {
  def main(args: Array[String]): Unit = {
    val conf = PipeConfig.newConfig()
    import bigglue.pipes.Implicits._
    val storeBuilder = DataStoreBuilder.load(conf)
    val s1 = new SolrDataMap[Cycleable, Cycleable](CycleableSerializer(), "s1")
    val s2 = new SolrDataMap[Cycleable, Cycleable](CycleableSerializer(), "s2")
    val s3 = storeBuilder.idmap[I[Int]]("s3")
    val pipe = s1 :--M1() --> s2 :--M2()--> (DataNode(s1), DataNode(s3))
    pipe.check(conf)
    pipe.init(conf)
    s1.put(Cycleable(0))
    Thread.sleep(20000)
    println(s"${s1.name}: $s1")
    println(s"${s2.name}: $s2")

    pipe.terminate()
  }
}
