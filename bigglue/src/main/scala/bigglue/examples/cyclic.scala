package bigglue.examples

import bigglue.computations.Mapper
import bigglue.configurations.{DataStoreBuilder, PipeConfig}
import bigglue.data.I
import bigglue.store.instances.InMemIdDataMap

/**
  * Created by chanceroberts on 12/14/17.
  */
case class M1() extends Mapper[I[Int], I[Int]](input => {
  List(I(input.a+1))
})

case class M2() extends Mapper[I[Int], I[String]](input => {
  List(I(s"${input.a}"))
})

case class M3() extends Mapper[I[Int], I[Int]](input => List(input))

object cyclic {
  def main(args: Array[String]): Unit = {
    val conf = PipeConfig.newConfig()
    import bigglue.pipes.Implicits._
    import bigglue.data.Implicits._
    val storeBuilder = DataStoreBuilder.load(conf)
    val s1 = storeBuilder.idmap[I[Int]]("s1")
    val s2 = storeBuilder.idmap[I[Int]]("s2")
    val s3 = storeBuilder.idmap[I[String]]("s3")
    val pipe = s1:--M1()-->s2 :< { (M2() --> s3) ~ (M3() --> s1) }
    pipe.check(conf)
    pipe.init(conf)
    s1.put(Seq(1).toIds())
    Thread.sleep(1000)
    println(s"${s3.name}: $s3")
  }
}
