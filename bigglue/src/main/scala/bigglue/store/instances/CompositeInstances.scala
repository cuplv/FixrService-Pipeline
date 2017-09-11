package bigglue.store.instances

import bigglue.store.{DataMap, DataStore}
import bigglue.data
import bigglue.data.Identifiable

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
          (dStoreL:DataStore[DataL], dStoreR:DataStore[DataR]) extends DataStore[bigglue.data.Either[DataL,DataR]] {

  def partition(ds: Seq[bigglue.data.Either[DataL,DataR]]): (Seq[DataL],Seq[DataR]) = {
    var dataLs = Seq.empty[DataL]
    var dataRs = Seq.empty[DataR]
    ds foreach {
      _ match {
        case bigglue.data.Left(d)  => dataLs = dataLs :+ d
        case bigglue.data.Right(d) => dataRs = dataRs :+ d
      }
    }
    (dataLs,dataRs)
  }

  override def put_(ds: Seq[bigglue.data.Either[DataL, DataR]]): Unit = {
    val pair = partition(ds)
    dStoreL.put(pair._1)
    dStoreR.put(pair._2)
  }

  override def all(): Seq[bigglue.data.Either[DataL, DataR]] = {
    dStoreL.all().map( bigglue.data.Left[DataL,DataR](_) ) ++ dStoreR.all().map( bigglue.data.Right[DataL,DataR](_) )
  }

  override def extract(): Seq[data.Either[DataL, DataR]] = {
    dStoreL.extract().map( bigglue.data.Left[DataL,DataR](_) ) ++ dStoreR.extract().map( bigglue.data.Right[DataL,DataR](_) )
  }

  override def size(): Int = dStoreL.size() + dStoreR.size()

  override def iterator(): Iterator[data.Either[DataL, DataR]] = new BothDataIterator[DataL,DataR](dStoreL.iterator(),dStoreR.iterator())

}

class BothDataIterator[DataL <: Identifiable[DataL],DataR <: Identifiable[DataR]](iterL: Iterator[DataL], iterR: Iterator[DataR]) extends Iterator[data.Either[DataL,DataR]] {

  override def next(): data.Either[DataL, DataR] = {
    if (iterL.hasNext) data.Left[DataL,DataR](iterL.next()) else data.Right[DataL,DataR](iterR.next())
  }

  override def hasNext: Boolean = iterL.hasNext || iterR.hasNext

}