package protopipes.examples

import protopipes.configurations.{Compute, PlatformBuilder}
import protopipes.configurations.instances.ThinActorPlatformBuilder
import protopipes.computations.Mapper
import protopipes.connectors.instances.{ActorConnector, IncrTrackerJobQueue}
import protopipes.data.I
import protopipes.data.Implicits._
import protopipes.pipes.St
import protopipes.platforms.UnaryPlatform
import protopipes.store.DataStore
import protopipes.store.instances.InMemDataStore
import com.typesafe.config.ConfigFactory

/**
  * Created by edmundlam on 8/8/17.
  */

case class Plus(n: Int)(implicit val platformBuilder: PlatformBuilder) extends Mapper[I[Int],I[Int]] {

  override def compute(input: I[Int]): List[I[Int]] = List( I(input.i + n) )

}

case class TimesPair(implicit val platformBuilder: PlatformBuilder) extends Mapper[protopipes.data.Pair[I[Int],I[Int]],I[Int]] {

  override def compute(input: protopipes.data.Pair[I[Int],I[Int]]): List[I[Int]] = List(I(input.left.i * input.right.i))

}

object sample {

  def main(args: Array[String]) : Unit = {

    implicit val defaultPlatformBuilder: PlatformBuilder = ThinActorPlatformBuilder

    import protopipes.data.Implicits._
    import protopipes.pipes.Implicits._

    val m0 = InMemDataStore.createIdDataMap[I[Int]]("m0")
    val m1 = InMemDataStore.createIdDataMap[I[Int]]("m1")
    val m2 = InMemDataStore.createIdDataMap[I[Int]]("m2")
    val m3 = InMemDataStore.createIdDataMap[I[Int]]("m3")
    val m4 = InMemDataStore.createIdDataMap[protopipes.data.Pair[I[Int],I[Int]]]("m4")
    val m5 = InMemDataStore.createIdDataMap[I[Int]]("m5")

    val config = ConfigFactory.load()

    val pipe = (m0 :--Plus(5)--> m2 || m1 :--Plus(10)--> m3) :-*Compute.cartesianProduct[I[Int],I[Int]]*-> m4 :--TimesPair()--> m5

    pipe.init(config)

    Thread.sleep(2000)

    m0.put(Seq(1,2,3).toIds)
    m1.put(Seq(4,5).toIds)

    Thread.sleep(4000)

    println(m0.name + " : " + m0.toString())
    println(m1.name + " : " + m1.toString())
    println(m2.name + " : " + m2.toString())
    println(m3.name + " : " + m3.toString())
    println(m4.name + " : " + m4.toString())
    println(m5.name + " : " + m5.toString())

    // val map = mapper.platformOpt.get.asInstanceOf[UnaryPlatform[I[Int],I[Int]]].getUpstreamConnector()
    //   .asInstanceOf[ActorConnector[I[Int]]].innerConnector.asInstanceOf[IncrTrackerJobQueue[I[Int]]].statusMap
    // val map = Probe.extractStatusMap(mapper)

    // println("Status: " + map.toString)

    pipe.terminate()

  }

}
