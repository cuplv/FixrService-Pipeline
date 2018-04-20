package bigglue.examples

import bigglue.computations.{Mapper, Reducer}
import bigglue.configurations.{DataStoreBuilder, PipeConfig}
import bigglue.data.{BasicIdentity, I, Identifiable, Identity}
import bigglue.data.serializers.JsonSerializer
import bigglue.store.instances.InMemDataMap
import bigglue.store.instances.solr.SolrDataMap
import spray.json._

/**
  * Created by chanceroberts on 4/2/18.
  * This is an example that was created to get a glimpse of BigGlue.
  * In practice, this is a very simple example, as it just adds 2, multiplies by 3, and then sums all bits together.
  */

/**
  * This is a simple [[Mapper]] step that adds 2 to the input.
  */
case object AA extends Mapper[I[Int], I[Int]](input => { List(I(input.a+2))}){
}


/** This is a simple [[Mapper]] step that multiplies the input by 3. */
case object BB extends Mapper[I[Int], I[Int]](input => { List(I(input.a*3))}){
  override val versionOpt = Some("0.1")
}

/** This is a simple [[Reducer]] step that sums up all of the inputs. */
case object CC extends Reducer[I[Int], Counter](i => BasicIdentity("sum"), i => curSum => Counter(i.a+curSum.sum), Counter(0))

case object IntProtocols extends DefaultJsonProtocol{
  implicit val iSerial = jsonFormat1(I[Int])
  implicit val counter = jsonFormat1(Counter)
}

case class IIntSerializer() extends JsonSerializer[I[Int]]{
  import IntProtocols.iSerial
  override def serializeToJson_(d: I[Int]): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): I[Int] = json.convertTo[I[Int]]
}

case class CounterSerializer() extends JsonSerializer[Counter]{
  import IntProtocols.counter
  override def serializeToJson_(d: Counter): JsObject = d.toJson.asJsObject

  override def deserialize_(json: JsObject): Counter = json.convertTo[Counter]
}

/** This is a simple [[Identifiable]] with the name "sum" which wraps an integer which is the sum. */
case class Counter(sum: Int) extends Identifiable[Counter]{
  override def mkIdentity(): Identity[Counter] = BasicIdentity("sum")
}

/**
  * This is the actual code for the simple example.
  * It first gets the configuration file with [[PipeConfig.newConfig]]
  * Then, it creates a few [[SolrDataMap]]s to put the data in within each step.
  * To start, it also puts 1, 2, 3, and 4 into a for the sake of having a starting point for the example.
  * Then, with a:--AA-->b:--BB-->c:-+CC+->d, it creates this:
  * [[bigglue.pipes.ReducerPipe]]([[bigglue.pipes.MapperPipe]]([[bigglue.pipes.MapperPipe]]([[bigglue.pipes.Implicits.DataNode]](a), AA, [[bigglue.pipes.Implicits.DataNode]](b)), BB, [[bigglue.pipes.Implicits.DataNode]](c)), CC, [[bigglue.pipes.Implicits.DataNode]](d))
  * Then, with a pipe, we run [[bigglue.pipes.Pipe.check]] and [[bigglue.pipes.Pipe.init]] to initialize the pipeline.
  * Finally, we run [[bigglue.pipes.Pipe.persist]] to start/resume the pipeline.
  */
object simpleExample {
  def main(args: Array[String]): Unit ={
    val conf = PipeConfig.newConfig()
    import bigglue.pipes.Implicits._
    val storeBuilder = DataStoreBuilder.load(conf)
    val a = new SolrDataMap[I[Int], I[Int]](IIntSerializer(), "a")
    val b = new SolrDataMap[I[Int], I[Int]](IIntSerializer(), "b")
    val c = new SolrDataMap[I[Int], I[Int]](IIntSerializer(), "c")
    val d = new SolrDataMap[Counter, Counter](CounterSerializer(), "d")
    val pipe = a:--AA-->b:--BB-->c:-+CC+->d
    pipe.check(conf)
    pipe.init(conf)
    // a.put(List(I(1), I(2), I(3), I(4)))
    pipe.persist()
    Thread.sleep(10000)
    println(d.all())
  }
}
