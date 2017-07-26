import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import pipecombi._

/**
  * Created by edmundlam on 6/20/17.
  */


object IDInt {

  def mkMap(ls: List[Int], name: String = "DataMap"): InMemDataMap[IDInt] = {
    val map = new InMemDataMap[IDInt](name = name)
    ls.map(
      i => map.put(IDInt(i))
    )
    // println("MkMap: " + map.identities)
    map
  }

}

case class IDInt(i : Int) extends Identifiable {
   override def identity(): Identity = Identity(i.toString,None)
}

case class PlusOne(conf: Any = "", name: String = "")(implicit system: ActorSystem) extends IncrTransformer[IDInt, IDInt](name, conf) {
  override val version = "0.1"

  override val statMap = new InMemDataMap[Stat]()
  override val provMap = new InMemDataMap[Identity]()
  override val errMap = new InMemDataMap[ErrorSummary]()

  override def compute(input: IDInt): List[IDInt] = List(IDInt(input.i + 1))

  override def toString: String = "_+1"
}

case class Plus(n: Int, conf: Any = "", name: String = "")(implicit system: ActorSystem) extends IncrTransformer[IDInt, IDInt](name, conf) {
  override val version = "0.1"

  override val statMap = new InMemDataMap[Stat]()
  override val provMap = new InMemDataMap[Identity]()
  override val errMap = new InMemDataMap[ErrorSummary]()

  override def compute(input: IDInt): List[IDInt] = List(IDInt(input.i + n))

  override def toString: String = s"_+$n"
}

case class PlusSleep(n: Int, conf: Any = "", name: String = "")(implicit system: ActorSystem) extends IncrTransformer[IDInt, IDInt](name, conf){
  override val version = "0.1"

  override val statMap = new InMemDataMap[Stat]()
  override val provMap = new InMemDataMap[Identity]()
  override val errMap = new InMemDataMap[ErrorSummary]()

  override def compute(input: IDInt): List[IDInt] = List(IDInt{Thread.sleep(input.i*1000); input.i + n})

  override def toString: String = s"_+$n"
}

case class TimesPair(conf: Any = "", name: String = "")(implicit system: ActorSystem) extends IncrTransformer[pipecombi.Pair[IDInt,IDInt], IDInt](name, conf) {
  override val version = "0.1"

  override val statMap = new InMemDataMap[Stat]()
  override val provMap = new InMemDataMap[Identity]()
  override val errMap = new InMemDataMap[ErrorSummary]()

  override def compute(input: pipecombi.Pair[IDInt,IDInt]) = {
    List(IDInt(input.left.i * input.right.i))
  }

  override def toString: String = "_*_"

}
/*
object BProduct extends Composer[IDInt, IDInt] {
  override val version = "0.1"

  override val statMap = new InMemDataMap[Stat]()
  override val provMap = new InMemDataMap[Identity]()
  override val errMap = new InMemDataMap[ErrorSummary]()

  override def compose(mapL: DataMap[IDInt], mapR: DataMap[IDInt]): DataMap[pipecombi.Pair[IDInt, IDInt]] = {
    val outMap = new InMemDataMap[pipecombi.Pair[IDInt,IDInt]]()
    mapL.items.map {
      inputL => mapR.items.map {
        inputR => { outMap.put(pipecombi.Pair(inputL,inputR)) }
      }
    }
    outMap
  }
}
*/
object Example1 {

   def main(args: Array[String]): Unit = {
     val conf = ConfigFactory.parseFile(new File("AkkaSpreadOutTest.conf"))
     val conf2 = "AkkaLocalTest.conf" //Some(ConfigFactory.parseFile(new File("AkkaLocalTest.conf")))
     //implicit val system = ActorSystem()
     val m0 = IDInt.mkMap(List(101,203), "m0")
     val m1 = IDInt.mkMap(List(10,27,34), "m1")
     val m2 = new InMemDataMap[IDInt](name = "m2")
     val m3 = new InMemDataMap[IDInt](name = "m3")
     val m4 = new InMemDataMap[IDInt](name = "m4")

     val m5 = new InMemDataMap[IDInt](name = "m5")
     val m6 = new InMemDataMap[IDInt](name = "m6")
     val m7 = new InMemDataMap[IDInt](name = "m7")
     val m8 = new InMemDataMap[IDInt](name = "m8")
     val m9 = IDInt.mkMap(List(6,2,4), "m9")
     val m10 = new InMemDataMap[IDInt](name = "m10")

     // { (m0 :--PlusOne--> m2) <-*BatchProduct.composer[IDInt,IDInt]*-> (m1 :--PlusOne--> m3) } :--TimesPair--> m4

     import Implicits._

     val pipe = { ((m0 :--PlusOne(conf, "step1")--> m2) <-*BatchProduct.composer[IDInt,IDInt]*-> (m1 :--PlusOne(conf2)--> m3)) :--TimesPair()--> m4 } :< {
       (PlusOne()--> m5 :--Plus(-10)--> m10) ~ (Plus(10)--> m6 :--Plus(100)--> m7 :--Plus(-12)--> m8)
     }

     //val pipe = m9 :--PlusSleep(1)--> m10
     //println(pipe)

     //pipe.run()
     //pipe.run(pipe.build())
     pipe.runConfig()
     Thread.sleep(100)

     println(s"m0: ${m0.toString}")
     println(s"m1: ${m1.toString}")
     println(s"m2: ${m2.toString}")
     println(s"m3: ${m3.toString}")
     println(s"m4: ${m4.toString}")
     println(s"m5: ${m5.toString}")
     println(s"m6: ${m6.toString}")
     println(s"m7: ${m7.toString}")
     println(s"m8: ${m8.toString}")
     println(s"m9: ${m9.toString}")
     println(s"m10: ${m10.toString}")
   }

}
