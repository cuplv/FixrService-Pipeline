package protopipes.examples

import com.typesafe.config.ConfigFactory
import protopipes.computations.Reducer
import protopipes.configurations.{DataStoreBuilder, PlatformBuilder}
import protopipes.configurations.instances.ThinActorPlatformBuilder
import protopipes.data.{BasicIdentity, I, Identifiable, Identity}
import protopipes.store.instances.InMemDataStore

/**
  * Created by edmundlam on 8/12/17.
  */

case class Count(word: String, count: Int) extends Identifiable[Count] {

  override def mkIdentity(): Identity[Count] = BasicIdentity(word)

}

case class CountOccurrence() extends Reducer[I[String],Count] {

  override val versionOpt = Some("v0.12")

  override def groupBy(input: I[String]): Identity[Count] = BasicIdentity(input.a)

  override def fold(input: I[String], output: Count): Count = Count(input.a, output.count + 1)

  override def zero(): Count = Count("", 0)

}

object wordcount {

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load()

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
