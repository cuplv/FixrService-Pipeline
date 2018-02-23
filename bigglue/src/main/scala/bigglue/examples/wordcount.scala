package bigglue.examples

import com.typesafe.config.ConfigFactory
import bigglue.computations.{Mapper, Reducer}
import bigglue.configurations.{DataStoreBuilder, PipeConfig, PlatformBuilder}
import bigglue.configurations.instances.ThinActorPlatformBuilder
import bigglue.data._
import bigglue.store.instances.InMemDataStore
import spray.json.DefaultJsonProtocol

/**
  * Created by edmundlam on 8/12/17.
  */

/*
import spray.json._

object CountToJson extends DefaultJsonProtocol {
  implicit val jsonCountFormat = jsonFormat2(Count)
}

import CountToJson._ */

case class Count(word: String, count: Int) extends Identifiable[Count] {

  override def mkIdentity(): Identity[Count] = BasicIdentity(word)

}

case class AddI() extends Mapper[I[String], I[String]](i =>{
  println(s"Computing ${i.a}!")
  List(I(s"${i.a}!"))
})

case class CountOccurrence() extends
  Reducer[I[String],Count](i => BasicIdentity(i.a), i => o => Count(i.a, o.count + 1), Count("",0)) {
  override val versionOpt = Some("v0.12")
}

case class MergeToOneWord() extends Reducer[Count, Count](input => {
  println(s"${input.word}, ${input.count} was sent!")
  BasicIdentity("word")
},
    i => o => {
      println(s"${i.count} added to ${o.count}!")
      Count("word", o.count+i.count)
    },
    Count("", 0)) {

}

case class TestReduction() extends Mapper[Count, Count](input => {
  println(s"$input was sent!")
  List(Count(input.word, input.count*2))
})

object wordcount {

  def main(args: Array[String]): Unit = {

    val config = PipeConfig.newConfig() // ConfigFactory.load()


    val testjson = Count("crap",10)

    // println(testjson.intoJson())

    // println( testjson.fromJson(testjson.intoJson()) )

    // implicit val defaultPlatformBuilder: PlatformBuilder = PlatformBuilder.load(config) // ThinActorPlatformBuilder

    import bigglue.data.Implicits._
    import bigglue.pipes.Implicits._

    val storeBuilder = DataStoreBuilder.load(config)
    val test = storeBuilder.list[I[String]]("test")
    val words  = storeBuilder.list[I[String]]("words") // InMemDataStore.createLinearStore[I[String]]("words")
    val counts = storeBuilder.idmap[Count]("counts") // InMemDataStore.createIdDataMap[Count]("counts")

    val wordcountpipe = words :-+CountOccurrence()+-> counts

    wordcountpipe.check(config)

    wordcountpipe.init(config)

    Thread.sleep(1000)

    val strs = Seq("rat","cat","cat","hat","crap","hello","rat","gun","hat")
    words.put(strs.toIds())

    Thread.sleep(1000)
    words.put(Seq("rat", "cat", "hello", "gun").toIds())

    Thread.sleep(10000)

    println(words.name + ": " + words.toString)
    println(counts.name + ": " + counts.toString)
    println(test.name + ": " + test.toString)

    //wordcountpipe.terminate()

  }

}
