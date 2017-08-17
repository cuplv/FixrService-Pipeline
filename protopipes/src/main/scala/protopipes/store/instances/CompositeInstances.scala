package protopipes.store.instances

import protopipes.store.{DataMap, DataStore}
import protopipes.data
import protopipes.data.Identifiable

/**
  * Created by edmundlam on 8/10/17.
  */

/*
class CompositeDataMap[KL,VL,KR,VR](mapL: DataMap[KL,VL], mapR: DataMap[KR,VR]) extends DataMap[data.Pair[KL,KR],data.Pair[VL,VR]] {

  override def put_(data: Seq[data.Pair[VL, VR]]): Unit = ???

  override def all(): Seq[data.Pair[VL, VR]] = ???

  override def extract(): Seq[data.Pair[VL, VR]] = ???

  override def put_(key: data.Pair[KL, KR], data: data.Pair[VL, VR]): Unit = ???

  override def get(key: data.Pair[KL, KR]): Option[data.Pair[VL, VR]] = ???

  override def remove(keys: Seq[data.Pair[KL, KR]]): Unit = ???

  override def remove(key: data.Pair[KL, KR]): Unit = ???

  override def size(): Int = ???

}
*/

case class BothDataStore[DataL <: Identifiable[DataL],DataR <: Identifiable[DataR]]
          (dStoreL:DataStore[DataL], dStoreR:DataStore[DataR]) extends DataStore[protopipes.data.Either[DataL,DataR]] {

  def partition(ds: Seq[protopipes.data.Either[DataL,DataR]]): (Seq[DataL],Seq[DataR]) = {
    var dataLs = Seq.empty[DataL]
    var dataRs = Seq.empty[DataR]
    ds foreach {
      _ match {
        case protopipes.data.Left(d)  => dataLs = dataLs :+ d
        case protopipes.data.Right(d) => dataRs = dataRs :+ d
      }
    }
    (dataLs,dataRs)
  }

  override def put_(ds: Seq[protopipes.data.Either[DataL, DataR]]): Unit = {
    val pair = partition(ds)
    dStoreL.put(pair._1)
    dStoreR.put(pair._2)
  }

  override def all(): Seq[protopipes.data.Either[DataL, DataR]] = {
    dStoreL.all().map( protopipes.data.Left[DataL,DataR](_) ) ++ dStoreR.all().map( protopipes.data.Right[DataL,DataR](_) )
  }

  override def extract(): Seq[data.Either[DataL, DataR]] = {
    dStoreL.extract().map( protopipes.data.Left[DataL,DataR](_) ) ++ dStoreR.extract().map( protopipes.data.Right[DataL,DataR](_) )
  }

  override def size(): Int = dStoreL.size() + dStoreR.size()

  override def iterator(): Iterator[data.Either[DataL, DataR]] = new BothDataIterator[DataL,DataR](dStoreL.iterator(),dStoreR.iterator())

}

class BothDataIterator[DataL,DataR](iterL: Iterator[DataL], iterR: Iterator[DataR]) extends Iterator[data.Either[DataL,DataR]] {

  override def next(): data.Either[DataL, DataR] = {
    if (iterL.hasNext) data.Left[DataL,DataR](iterL.next()) else data.Right[DataL,DataR](iterR.next())
  }

  override def hasNext: Boolean = iterL.hasNext || iterR.hasNext

}