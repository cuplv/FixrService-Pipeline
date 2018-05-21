import bigglue.computations.{Mapper, Reducer}
import bigglue.configurations.{DataStoreBuilder, PipeConfig}
import bigglue.data.{BasicIdentity, Identifiable, Identity}

import scala.util.Random

/**
  * Created by chanceroberts on 2/4/18.
  */


object Demo {
  case class A(location: String) extends Identifiable[A]{
    override def mkIdentity(): Identity[A] = BasicIdentity(location)
  }

  case class B(randInt: Int, randInt2: Int) extends Identifiable[B]{
    override def mkIdentity(): Identity[B] = BasicIdentity(s"($randInt, $randInt2)")
  }

  case class ListedB(from: Option[String], randList: List[Int]) extends Identifiable[ListedB]{
    override def mkIdentity(): Identity[ListedB] = BasicIdentity(s"ListedB($from)")
  }

  case object AToB extends Mapper[A, B](input => {
    val r = Random
    def generateNumber(times: Int): Int = times match {
      case 0 => 0
      case _ => r.nextInt(1000) + generateNumber(times-1)
    }
    List(B(generateNumber(input.location.length), r.nextInt(10000)))
  })

  case object ListTheBs extends Reducer[B, ListedB](input => input.getProvenance match{
    case Some(x: String) => BasicIdentity(x)
    case _ => BasicIdentity("")
  },
    i => o => o.from match{
      case None => ListedB(i.getEmbedded("provInfo"), o.randList ++ List(i.randInt+i.randInt2))
      case a => ListedB(a, o.randList ++ List(i.randInt+i.randInt2))
    }, ListedB(None, List[Int]()))

  def main(args: Array[String]): Unit = {
    import bigglue.pipes.Implicits._
    val conf = PipeConfig.newConfig()
    val storeBuilder = DataStoreBuilder.load(conf)
    val aMap = storeBuilder.idmap[A]("D0")
    val bMap = storeBuilder.idmap[B]("D1")
    val bListMap = storeBuilder.idmap[ListedB]("D2")
    val provPipe = aMap :--AToB--> bMap
    val pipe = aMap :--AToB--> bMap :-+ListTheBs+-> bListMap
    pipe.check(conf)
    pipe.init(conf)
    def generatePuttingInMaps(a: List[String], times: Int = 1): Unit = a match{
      case Nil => ()
      case head :: tail =>
        def putInMap(timesLeft: Int, string: String): Unit = timesLeft match{
          case 0 => ()
          case _ =>
            aMap.put(A(string))
            putInMap(timesLeft-1, string)
        }
        putInMap(times, head)
        Thread.sleep(1000)
        generatePuttingInMaps(tail, times+1)
    }
    generatePuttingInMaps(List("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven",
      "facebook", "spacex", "google"))
    Thread.sleep(4000)
    println(aMap)
    println(bMap)
    println(bListMap)
  }
}
