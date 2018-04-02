package bigglue.examples

import bigglue.computations.Mapper
import bigglue.configurations.{DataStoreBuilder, PipeConfig}
import bigglue.data.{BasicIdentity, I, Identifiable, Identity}

/**
  * Created by chanceroberts on 12/15/17.
  */

case class Aa(s: String) extends Identifiable[Aa]{
  override def mkIdentity(): Identity[Aa] = BasicIdentity(s)
}

case class Bb(s: String) extends Identifiable[Bb]{
  override def mkIdentity(): Identity[Bb] = BasicIdentity(s)
}

case class Cc(i: Int, j: String) extends Identifiable[Cc]{
  override def mkIdentity(): Identity[Cc] = BasicIdentity(s"$j://$i")
}

case class Simple() extends Mapper[Aa, I[(Bb, Cc)]](input => {
  val s = input.s
  List(I((Bb(s"%s."), Cc(s.length, s"$s."))))
})

case class BHeavyComputation() extends Mapper[Bb, Bb](input => {
  println(s"Heavy Computation in B for ${input.s}.")
  Thread.sleep(3000)
  List(input)
})

object branch {
  def main(args: Array[String]): Unit = {
    val conf = PipeConfig.newConfig()
    import bigglue.pipes.Implicits._
    val storeBuilder = DataStoreBuilder.load(conf)
    val aStore = storeBuilder.idmap[Aa]("a")
    aStore.put(List(Aa("test"), Aa("test2"), Aa("test3"), Aa("test4"), Aa("test5")))
    val bStore = storeBuilder.idmap[Bb]("b")
    val bCompute = storeBuilder.idmap[Bb]("b2")
    val cStore = storeBuilder.idmap[Cc]("c")
    val pipe = aStore:--Simple()--> (bStore:--BHeavyComputation()-->bCompute, DataNode(cStore))
    pipe.check(conf)
    pipe.init(conf)
    pipe.run()
    Thread.sleep(10000)
    println(aStore)
    println(bStore)
    println(cStore)
  }
}
