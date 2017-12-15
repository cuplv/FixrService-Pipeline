package bigglue.examples

import bigglue.computations.Mapper
import bigglue.configurations.{DataStoreBuilder, PipeConfig}
import bigglue.data.{BasicIdentity, Identifiable, Identity}
import bigglue.store.instances.InMemDataStore

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

case class Dd(a: Aa, b: Bb, c: Cc) extends Identifiable[Dd]{
  override def mkIdentity(): Identity[Dd] = BasicIdentity(s"A: ${a.getId()}, B: ${b.getId()}, C: ${c.getId()}")
}

case class SplitStep() extends Mapper[Aa, Amalgamation](input => {
  val s = input.s
  List(Amalgamation(Bb(s"$s."), Cc(s.length, s"$s."), Dd(input, Bb(s"$s."), Cc(s.length, s"$s."))))
})

case class Amalgamation(b: Bb, c: Cc, d: Dd) extends Identifiable[Amalgamation]{
  override def mkIdentity(): Identity[Amalgamation] = BasicIdentity(s"(${b.getId()}, ${c.getId()}, ${d.getId()}")
}

case class SplitToB() extends Mapper[Amalgamation, Bb](input => List(input.b))

case class SplitToC() extends Mapper[Amalgamation, Cc](input => List(input.c))

case class SplitToD() extends Mapper[Amalgamation, Dd](input => List(input.d))

object branch {
  def main(args: Array[String]): Unit = {
    val conf = PipeConfig.newConfig()
    import bigglue.pipes.Implicits._
    val storeBuilder = DataStoreBuilder.load(conf)
    val aStore = storeBuilder.idmap[Aa]("a")
    val bStore = storeBuilder.idmap[Bb]("b")
    val cStore = storeBuilder.idmap[Cc]("c")
    val dStore = storeBuilder.idmap[Dd]("d")
    val mergeStore = storeBuilder.idmap[Amalgamation]("am")
    val pipe = aStore:--SplitStep()--> mergeStore :< {(SplitToB()-->bStore) ~ (SplitToC()-->cStore) ~ (SplitToD() --> dStore)}
    pipe.check(conf)
    pipe.init(conf)
    aStore.put(List(Aa("test"), Aa("test2"), Aa("test3")))
    Thread.sleep(4000)
    println(aStore)
    println(bStore)
    println(cStore)
    println(dStore)
  }
}
