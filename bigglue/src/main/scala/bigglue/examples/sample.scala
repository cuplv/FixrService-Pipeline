package bigglue.examples

import bigglue.configurations.{Compute, DataStoreBuilder, PipeConfig, PlatformBuilder}
import bigglue.configurations.instances.ThinActorPlatformBuilder
import bigglue.computations.{Mapper}
import bigglue.connectors.instances.{ActorConnector, IncrTrackerJobQueue}
import bigglue.data.I
import bigglue.data.Implicits._
import bigglue.pipes.St
import bigglue.platforms.UnaryPlatform
import bigglue.store.DataStore
import bigglue.store.instances.InMemDataStore
import com.typesafe.config.ConfigFactory

/**
  * Created by edmundlam on 8/8/17.
  */

case class Plus(n: Int) extends Mapper[I[Int],I[Int]](input => List(I(input.i + n)) ) {
  override val versionOpt: Option[String] = Some("v0.5")
  //  override def compute(input: I[Int]): List[I[Int]] = List( I(input.i + n) )
}

case class TimesPair() extends Mapper[bigglue.data.Pair[I[Int],I[Int]],I[Int]](pair => List( I(pair.left.i * pair.right.i) )) {
  override val versionOpt: Option[String] = Some("v0.6")
  // override def compute(input: protopipes.data.Pair[I[Int],I[Int]]): List[I[Int]] = List(I(input.left.i * input.right.i))
}

case class Helloworld() extends Mapper[I[Int],I[String]](input => List(I(s"Hello! ${input.i}"))) {
  override val versionOpt: Option[String] = Some("v0.5")
  // override def compute(input: I[Int]): List[I[String]] = List(I(s"Hello! ${input.i()}"))
}

object sample {

  def main(args: Array[String]) : Unit = {

    // implicit val defaultPlatformBuilder: PlatformBuilder = ThinActorPlatformBuilder

    import bigglue.data.Implicits._
    import bigglue.pipes.Implicits._

    // val config = ConfigFactory.load()
    val config = PipeConfig.newConfig()

    val storeBuilder = DataStoreBuilder.load(config)

    val d0 = storeBuilder.idmap[I[Int]]("D0") // InMemDataStore.createIdDataMap[I[Int]]("m0")
    val d1 = storeBuilder.idmap[I[Int]]("D1") // InMemDataStore.createIdDataMap[I[Int]]("m1")
    val d2 = storeBuilder.idmap[I[Int]]("D2") // InMemDataStore.createIdDataMap[I[Int]]("m2")
    val d22 = storeBuilder.idmap[I[Int]]("D2!")
    val d3 = storeBuilder.idmap[I[Int]]("D3") // InMemDataStore.createIdDataMap[I[Int]]("m3")
    val d4 = storeBuilder.idmap[bigglue.data.Pair[I[Int],I[Int]]]("D4") // InMemDataStore.createIdDataMap[protopipes.data.Pair[I[Int],I[Int]]]("m4")
    val d5 = storeBuilder.idmap[I[Int]]("D5") // InMemDataStore.createIdDataMap[I[Int]]("m5")
    val d6 = storeBuilder.idmap[I[Int]]("D6") // InMemDataStore.createIdDataMap[I[Int]]("m6")
    val d7 = storeBuilder.idmap[I[String]]("D7") // InMemDataStore.createIdDataMap[I[String]]("m7")
    val d8 = storeBuilder.idmap[I[Int]]("D8") // InMemDataStore.createIdDataMap[I[Int]]("m8")
    val d9 = storeBuilder.idmap[I[Int]]("D9") // InMemDataStore.createIdDataMap[I[Int]]("m9")
    val d10 = storeBuilder.idmap[I[Int]]("D10") // InMemDataStore.createIdDataMap[I[Int]]("m10")
    val d11 = storeBuilder.idmap[bigglue.data.Pair[I[Int],I[Int]]]("D11") // InMemDataStore.createIdDataMap[protopipes.data.Pair[I[Int],I[Int]]]("m11")

    val pipe = (d0 :--Plus(5)--> d2 :--Plus(10)-->d10 || d1 :--Plus(10)--> d3) :-*Compute.cartesianProduct[I[Int],I[Int]]*-> d4 :--TimesPair()--> d5 :< {
      (Plus(40)--> d6 :--Plus(-20)--> d8) ~ (Helloworld()--> d7)
    }

    //val pipe2 = (d8 || d9 :--Plus(15)--> d10) :-*Compute.cartesianProduct[I[Int],I[Int]]*-> d11

    pipe.init(config)
    //pipe2.init(config)

    Thread.sleep(2000)

    d0.put(Seq(1,2,3).toIds)
    d1.put(Seq(4,5).toIds)
    d9.put(Seq(10,12).toIds)

    Thread.sleep(4000)

    println(d0.name + " : " + d0.toString())
    println(d1.name + " : " + d1.toString())
    println(d2.name + " : " + d2.toString())
    println(d3.name + " : " + d3.toString())
    println(d4.name + " : " + d4.toString())
    println(d5.name + " : " + d5.toString())
    println(d6.name + " : " + d6.toString())
    println(d7.name + " : " + d7.toString())
    println(d8.name + " : " + d8.toString())
    println(d9.name + " : " + d9.toString())
    println(d10.name + " : " + d10.toString())
    println(d11.name + " : " + d11.toString())

    // val map = mapper.platformOpt.get.asInstanceOf[UnaryPlatform[I[Int],I[Int]]].getUpstreamConnector()
    //   .asInstanceOf[ActorConnector[I[Int]]].innerConnector.asInstanceOf[IncrTrackerJobQueue[I[Int]]].statusMap
    // val map = Probe.extractStatusMap(mapper)

    // println("Status: " + map.toString)

    pipe.terminate()
    //pipe2.terminate()

  }

}
