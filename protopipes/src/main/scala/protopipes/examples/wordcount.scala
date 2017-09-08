package protopipes.examples

import com.typesafe.config.ConfigFactory
import protopipes.computations.Reducer
import protopipes.configurations.{DataStoreBuilder, PipeConfig, PlatformBuilder}
import protopipes.configurations.instances.ThinActorPlatformBuilder
import protopipes.data._
import protopipes.store.instances.InMemDataStore
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

case class CountOccurrence() extends
  Reducer[I[String],Count](i => BasicIdentity(i.a), i => o => Count(i.a, o.count + 1), Count("",0)) {
  override val versionOpt = Some("v0.12")
}

object wordcount {

  def main(args: Array[String]): Unit = {

    val config = PipeConfig.newConfig() // ConfigFactory.load()


    val testjson = Count("crap",10)

    // println(testjson.intoJson())

    // println( testjson.fromJson(testjson.intoJson()) )

    // implicit val defaultPlatformBuilder: PlatformBuilder = PlatformBuilder.load(config) // ThinActorPlatformBuilder

    import protopipes.data.Implicits._
    import protopipes.pipes.Implicits._

    val storeBuilder = DataStoreBuilder.load(config)

    val words  = storeBuilder.list[I[String]]("words") // InMemDataStore.createLinearStore[I[String]]("words")
    val counts = storeBuilder.idmap[Count]("counts") // InMemDataStore.createIdDataMap[Count]("counts")

    val wordcountpipe = words :-+CountOccurrence()+-> counts

    wordcountpipe.check(config)

    wordcountpipe.init(config)

    Thread.sleep(2000)

    val strs = Seq("rat","cat","cat","hat","crap","hello","rat","gun","hat")
    words.put(strs.toIds())

    Thread.sleep(4000)

    println(words.name + ": " + words.toString)
    println(counts.name + ": " + counts.toString)

    wordcountpipe.terminate()

  }

}
