package bigglue.examples

import bigglue.computations.Reducer
import bigglue.configurations.PipeConfig
import bigglue.data.{BasicIdentity, I}
import bigglue.store.instances.{InMemDataMap, InMemDataQueue}

/**
  * Created by chanceroberts on 7/3/18.
  */

case class testSum() extends Reducer[I[Int], I[Int]](
  input => if (input.a%2 == 0) List(BasicIdentity("even")) else List(BasicIdentity("odd")),
  i => o => I(i.a+o.a),
  I(0)
)

case class addEvenOdd() extends Reducer[I[Int], I[Int]](
  input => List(BasicIdentity("totalSum")),
  i => o => I(i.a+o.a),
  I(0)
)

object doublereduction {

  def main(args: Array[String]): Unit = {
    val conf = PipeConfig.newConfig()
    val origStore = new InMemDataQueue[I[Int]]
    origStore.put_(List(I(1), I(2), I(3), I(4)))
    val interStore = new InMemDataMap[I[Int], I[Int]]
    interStore.setName("interStore")
    val outerStore = new InMemDataMap[I[Int], I[Int]]
    origStore.setName("origStore")
    import bigglue.pipes.Implicits._
    val pipe = origStore :-+testSum()+-> interStore :-+addEvenOdd()+-> outerStore
    pipe.check(conf)
    pipe.init(conf)
    pipe.persist()
    Thread.sleep(1000)
    println(origStore.all())
    println(interStore.map)
    println(outerStore.map)
    origStore.put(List(I(5)))
    Thread.sleep(1000)
    println(origStore.all())
    println(interStore.map)
    println(outerStore.map)
    // Expected Result is 15.
  }
}
