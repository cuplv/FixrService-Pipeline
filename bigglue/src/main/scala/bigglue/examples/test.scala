package bigglue.examples

import java.util.Calendar

import com.typesafe.config.ConfigFactory
import bigglue.configurations.{Constant, DataStoreBuilder, PipeConfig, PlatformBuilder}
import bigglue.data._
import bigglue.data.serializers.{BasicSerializer, IStringBasicSerializer}
import bigglue.platforms.instances.thinactors.ThinActorUnaryPlatform
import bigglue.platforms.instances.thinactors.ThinActorMapperPlatform
import bigglue.store.{DataMap, DataQueue, DataStore}
import bigglue.store.instances.InMemDataMap
import bigglue.store.instances.kafka.{KafkaConfig, KafkaDataMap, KafkaQueue}

/**
  * Created by edmundlam on 8/10/17.
  */

case class A(i: Int) {

  var x = 2

  def set(y: Int): Unit = x = y

  override def equals(obj: scala.Any): Boolean = obj.asInstanceOf[A].x == x

  override def toString: String = s"A($i)#$x"

}

class B(i: Int) {

  def get(): Int = i

  override def equals(obj: scala.Any): Boolean = {
    // obj.asInstanceOf[B].get() == i
    obj.hashCode() == this.hashCode()
  }

}

object test {

  def main(args: Array[String]): Unit = {

    val b1a = new B(1)
    val b1b = new B(1)

    var map = Map.empty[B,Int]

    map = map + (b1b -> 2)

    println(map)

    println(map.get(b1b))

    val a1a = new A(2)
    val a1b = new A(2)
    a1a.set(3)
    a1b.set(4)

    var ls = Map.empty[A,A]

    ls = ls + (a1a -> a1a, a1b -> a1b)

    println(ls)

    println(ls.get(a1b))

    println(a1b.asInstanceOf[Any] == a1b.asInstanceOf[Any])

    val constructor = Class.forName("protopipes.platforms.instances.thinactors.ThinActorMapperPlatform")
      .getConstructors()(0)

    println(constructor)

    val args = Array[AnyRef]("test")

    val platform = constructor.newInstance( args:_* ).asInstanceOf[ThinActorUnaryPlatform[I[Int],I[Int]]]

    println(platform)

    val config = PipeConfig.newConfig() // ConfigFactory.load()

    PlatformBuilder.load(config)

    println(config)

    val count_step_config = PipeConfig.liftTypeSafeConfigToLabel(config.typeSafeConfig,"count-step")
    println(count_step_config.getConfig(Constant.BIGGLUE).getConfig(Constant.PLATFORM))

  }

}

object test2 {

  def main(args: Array[String]): Unit = {

    val now = Calendar.getInstance().getTime()

    println(now)

    val map = new InMemDataMap[Identity[I[Int]],I[Int]]

    println(map.isInstanceOf[DataQueue[_]])

  }

}

object test3 {

  def main(args: Array[String]): Unit = {

    // import protopipes.store.instances.InMemDataMap

    val queue = new KafkaQueue[I[String]] {

      // override val serializer: DataSerializer[I[String]] = IStringSerializer

      override val serializer: BasicSerializer[I[String]] = IStringBasicSerializer

    }

    queue.setName("test-data-queue")

    queue.topic("test-data6-queue")

    val conf = PipeConfig.newConfig() // ConfigFactory.load()

    queue.init(conf)

    queue.put(I("testing 1 2 3"))
    queue.put(I("hello world 3 2 1"))

    val m3 = I("crap")
    m3.setVersion("v0.2")
    queue.put(m3)

    while(queue.size() > 0) {
      val m = queue.dequeue().get
      println(s"id: ${m.identity()} , data: $m, EQ: ${m.identity() == (I("testing 1 2 3").identity())}")
    }

  }

}

object test4 {

  def main(args: Array[String]): Unit = {

    val conf = PipeConfig.newConfig()

    val kconf = KafkaConfig.resolveKafkaConfig("count-step", conf)

    println(kconf)

  }

}

object test5 {

  def main(args: Array[String]): Unit = {

    val map = new KafkaDataMap[String,I[String]] {

      // override val serializer: DataSerializer[I[String]] = IStringSerializer

      override val serializer: BasicSerializer[I[String]] = IStringBasicSerializer

    }

    map.setName("count-step")

    map.topic("test-data-map")

    val conf = PipeConfig.newConfig() // ConfigFactory.load()

    map.init(conf)

    map.put("1", I("this is it"))
    map.put("2", I("ha ha ha"))
    map.put("20", I("Funny"))
    map.put("2", I("ha ha again"))
    map.put("1", I("this is it again"))

    println("1: " + map.get("1").get)
    println("2: " + map.get("2").get)
    println("20: " + map.get("20").get)

    println("1: " + map.dequeue().get)
    println("2: " + map.dequeue().get)
    println("20: " + map.dequeue().get)
    println("2: " + map.dequeue().get)
    println("1: " + map.dequeue().get)

  }
}