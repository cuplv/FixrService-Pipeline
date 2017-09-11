package bigglue.store.instances

import bigglue.data.{BasicIdentity, Identifiable, Identity}
import bigglue.exceptions.CallNotAllowException
import bigglue.store._

/**
  * Created by edmundlam on 8/8/17.
  */

object InMemDataStore {

  def createIdDataMap[Data <: Identifiable[Data]](name: String): DataStore[Data] = {
    val map = new InMemIdDataMap[Data]
    map.setName(name)
    map
  }

  def createDataMultiMap[K, V](name: String): DataMultiMap[K, V] = {
    val map = new InMemDataMultiMap[K, V]
    map.setName(name)
    map
  }

  def createLinearStore[Data](name: String): DataStore[Data] = {
    val store = new InMemLinearStore[Data]
    store.setName(name)
    store
  }

}

class InMemLinearStore[Data] extends DataStore[Data] {

  var ls = List.empty[Data]

  override def put_(data: Seq[Data]): Unit = ls = ls ++ data

  override def all(): Seq[Data] = ls

  override def extract(): Seq[Data] = {
    val is = ls
    ls = List.empty[Data]
    is
  }

  override def size(): Int = ls.size

  override def iterator(): Iterator[Data] = ls.iterator

  override def toString: String = ls.mkString("{",",","}")

}

class InMemDataQueue[Data] extends DataQueue[Data] {

  var ls = Vector.empty[Data]

  override def put_(data: Seq[Data]): Unit = ls = ls ++ data

  override def all(): Seq[Data] = ls

  override def dequeue(): Option[Data] = {
    if (ls.size > 0) {
      val l = ls.head
      ls = ls.tail
      Some(l)
    } else {
      None
    }
  }

  override def extract(): Seq[Data] = {
    val is = ls
    ls = Vector.empty[Data]
    is
  }

  override def size(): Int = ls.size

  override def iterator(): Iterator[Data] = ls.iterator

}


class InMemDataMap[Key, Data] extends DataMap[Key, Data] {

  var map = Map.empty[Key, Data]

  override def put_(data: Seq[Data]): Unit =
     throw new CallNotAllowException("\'put\' in Data Map needs to specify input key.", None)

  override def all(): Seq[Data] = map.values.toSeq

  override def put_(key: Key, data: Data): Unit = {
    map = map + (key -> data)
  }

  override def get(key: Key): Option[Data] = map.get(key)

  override def contains(key: Key): Boolean = map.contains(key)

  override def extract(): Seq[Data] = {
    val is = map.values
    map = Map.empty[Key, Data]
    is.toSeq
  }

  override def remove(keys: Seq[Key]): Unit = map = map -- keys

  override def remove(key: Key): Unit = map = map - key

  override def size(): Int = map.size

  override def iterator(): Iterator[Data] = map.values.iterator

}

class InMemDataMultiMap[Key, Data] extends DataMultiMap[Key, Data] {

  var map = Map.empty[Key, Set[Data]]

  override def put_(data: Seq[Set[Data]]): Unit = {
    // TODO: Throw exception
    ???
  }

  override def all(): Seq[Set[Data]] = map.values.toSeq

  override def put_(key: Key, data: Set[Data]): Unit = {
    map = map + (key -> (map.getOrElse(key, Set.empty[Data]) ++ data))
  }

  override def get(key: Key): Set[Data] = map.getOrElse(key, Set.empty[Data])

  override def extract(): Seq[Set[Data]] = {
    val ls = map.values
    map = Map.empty[Key, Set[Data]]
    ls.toSeq
  }

  override def contains(key: Key, data: Set[Data]): Boolean = data subsetOf get(key)

  override def remove(keys: Seq[Key]): Unit = map = map -- keys

  override def remove(key: Key): Unit = map = map - key

  override def remove(key: Key, data: Set[Data]): Unit = {
    if (map.contains(key)) {
      val newSet = map.get(key).get -- data
      map = map + (key -> newSet)
    }
  }

  override def remove(data: Set[Data]): Unit = map.keys foreach { remove(_, data) }

  override def toString: String = map.toList.map( p => s"${p._1} -> ${p._2.mkString("{",",","}")}" ).mkString(" ; ")

  override def size(): Int = map.size

  override def iterator(): Iterator[Set[Data]] = map.values.iterator

  override def iterator(key: Key): Iterator[Data] = map.get(key).get.iterator

}

class InMemIdDataMap[Data <: Identifiable[Data]] extends IdDataMap[Data] {

  var map = Map.empty[Identity[Data],Data]

  override def put_(data: Seq[Data]): Unit = {
    data foreach {
      d => map = map + (d.identity() -> d)
    }
  }

  override def all(): Seq[Data] = map.values.toSeq

  override def put_(key: Identity[Data], data: Data): Unit = {
    map = map + (key -> data)
  }

  override def get(key: Identity[Data]): Option[Data] = map.get(key)

  override def contains(key: Identity[Data]): Boolean = map.contains(key)

  override def extract(): Seq[Data] = {
    val is = map.values
    map = Map.empty[Identity[Data], Data]
    is.toSeq
  }

  override def remove(keys: Seq[Identity[Data]]): Unit = {
    map = map -- keys
  }

  override def remove(key: Identity[Data]): Unit = {
    map = map - key
  }

  override def size(): Int = map.size

  override def iterator(): Iterator[Data] = map.values.iterator

  override def toString: String = if (map.size > 0) map.values.mkString("{",", ","}") else "<None>"

}