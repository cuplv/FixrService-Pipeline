package protopipes.store

import com.typesafe.config.Config
import protopipes.checkers.ConfigChecker
import protopipes.configurations.PipeConfig
import protopipes.connectors.{Connector, Upstream}
import protopipes.data.{BasicIdentity, Identifiable, Identity}

/**
  * Created by edmundlam on 8/8/17.
  */


abstract class DataStore[Data] extends Upstream[Data] with ConfigChecker  {

  var name = "DataStore"

  def displayName(): String = name

  def setName(newName: String) : Unit = name = newName

  def init(conf: PipeConfig): Unit = { }

  def terminate(): Unit = { }

  def put_(data: Seq[Data]): Unit

  def all(): Seq[Data]

  def extract(): Seq[Data]

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