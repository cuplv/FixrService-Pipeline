package bigglue.store

import com.typesafe.config.Config
import bigglue.checkers.ConfigChecker
import bigglue.configurations.{ConfigBuildsDataStore, PipeConfig}
import bigglue.connectors.{Connector, Upstream}
import bigglue.data.serializers.BasicSerializer
import bigglue.data.{BasicIdentity, Identifiable, Identity}

/**
  * Created by edmundlam on 8/8/17.
  */

/**
  * The Data Store is an abstraction for how you store your data in BigGlue.
  * Within Data Stores, you have [[DataMap]], [[DataMultiMap]], and [[DataQueue]] from a list of most useful to least useful within BigGlue.
  * Within the example, all of the data stores are [[bigglue.store.instances.solr.SolrDataMap]]s.
  * @tparam Data The type of the data that is intended to be put into the data store. For a, b, and c, this is [[bigglue.data.I]][Int].
  *              For d, this is [[bigglue.examples.Counter]].
  */
abstract class DataStore[Data] extends Upstream[Data] with ConfigChecker with ConfigBuildsDataStore  {

  //val serializerOpt: Option[BasicSerializer[Data]] = None

  var name = "DataStore"

  def displayName(): String = name

  def setName(newName: String) : Unit = name = newName

  def init(conf: PipeConfig): Unit = { }

  def terminate(): Unit = { }

  /**
    * This actually puts the data into the data store.
    * @param data The data to be put into the data store.
    */
  def put_(data: Seq[Data]): Unit

  def all(): Seq[Data]

  def extract(): Seq[Data]

  /**
    * This puts the data into the data store using [[put_]], and then sends the data down the
    * pipeline if the data store is connected to the pipeline.
    * @param data The data that needs to be put into the data store.
    */
  def put(data: Seq[Data]): Unit = {
     put_(data)
     transmitDownstream(data)
  }

  def put(data: Data): Unit = put(Seq(data))

  def add(data: Data): Unit = put(Seq(data))

  def size(): Int

  def iterator(): Iterator[Data]


  /*
  override def registerConnector(connector: Connector[Data]): Unit = {
    super.registerConnector(connector)
    // connector.registerStore(this)
  } */

}

abstract class DataQueue[Data] extends DataStore[Data] {

  def dequeue(): Option[Data]

  def enqueue(data: Data): Unit = put(data)

  def enqueue(data: Seq[Data]): Unit = put(data)

}

abstract class DataMap[Key,Data] extends DataStore[Data] {

  def put_(key: Key, data: Data): Unit

  def get(key: Key): Option[Data]

  def contains(key: Key): Boolean

  def remove(keys: Seq[Key]): Unit

  def remove(key: Key): Unit

  def put(key: Key, data: Data): Unit = {
    val existed = contains(key)
    put_(key, data)
    if (!existed) transmitDownstream(data) else transmitDownstreamModified(data)
  }

  def getOrElse(key: Key, default: Data): Data = get(key) match {
    case Some(data) => data
    case None => default
  }

}

abstract class DataMultiMap[Key,Data] extends DataStore[Set[Data]] {

  def put_(key: Key, data: Set[Data]): Unit

  def get(key: Key): Set[Data]

  def put(key: Key, data: Set[Data]): Unit = {
    put_(key, data)
    transmitDownstream(data)
  }

  def contains(key: Key, data: Set[Data]): Boolean

  def contains(key: Key, data: Data): Boolean = contains(key, Set(data))

  def remove(keys: Seq[Key]): Unit

  def remove(key: Key): Unit

  def remove(key: Key, data: Set[Data]): Unit

  def remove(data: Set[Data]): Unit

  def iterator(key: Key): Iterator[Data]

}

abstract class IdDataMap[Data <: Identifiable[Data]] extends DataMap[Identity[Data],Data]