package bigglue.store

import bigglue.data.I
import bigglue.exceptions.CallNotAllowException

/**
  * Created by chanceroberts on 1/15/18.
  */

class TupleDataStore[DataL, DataR](dataStoreL: DataStore[DataL], dataStoreR: DataStore[DataR])
  extends DataStore[I[(DataL, DataR)]]{
  override def put_(data: Seq[I[(DataL, DataR)]]): Unit = {
    val (dataL, dataR) = data.foldRight(List[DataL](), List[DataR]()){
      case (I((dL, dR)), (lL, lR)) => (dL :: lL, dR :: lR)
    }
    dataStoreL.put(dataL)
    dataStoreR.put(dataR)
  }

  override def iterator(): Iterator[I[(DataL, DataR)]] =
    throw new CallNotAllowException("Iterators not supported in TupleDataStores.", None)

  override def all(): Seq[I[(DataL, DataR)]] = {
    val dataLAll = dataStoreL.all()
    val dataRAll = dataStoreR.all()
    dataLAll.foldRight(List[I[(DataL, DataR)]]()){
      case (l, lis) => dataRAll.foldRight(List[I[(DataL, DataR)]]()){
        case (r, rLis) => I(l, r) :: rLis
      } ::: lis
    }
  }

  override def extract(): Seq[I[(DataL, DataR)]] = {
    val dataLAll = dataStoreL.extract()
    val dataRAll = dataStoreR.extract()
    dataLAll.foldRight(List[I[(DataL, DataR)]]()){
      case (l, lis) => dataRAll.foldRight(List[I[(DataL, DataR)]]()){
        case (r, rLis) => I(l, r) :: rLis
      } ::: lis
    }
  }

  override def size(): Int = dataStoreL.size*dataStoreR.size
}

class TupleDataMap[KeyL, KeyR, DataL, DataR](dataMapL: DataMap[KeyL, DataL], dataMapR: DataMap[KeyR, DataR])
  extends DataMap[I[(KeyL, KeyR)], I[(DataL, DataR)]] {
  override def put_(data: Seq[I[(DataL, DataR)]]): Unit = {
    val (dataL, dataR) = data.foldRight(List[DataL](), List[DataR]()){
      case (I((dL, dR)), (lL, lR)) => (dL :: lL, dR :: lR)
    }
    dataMapL.put(dataL)
    dataMapR.put(dataR)
  }

  override def put_(key: I[(KeyL, KeyR)], data: I[(DataL, DataR)]): Unit = {
    dataMapL.put(key.a._1, data.a._1)
    dataMapR.put(key.a._2, data.a._2)
  }

  override def remove(keys: Seq[I[(KeyL, KeyR)]]): Unit = keys.foreach(x => remove(x))

  override def remove(key: I[(KeyL, KeyR)]): Unit = {
    dataMapL.remove(key.a._1)
    dataMapR.remove(key.a._2)
  }

  override def iterator(): Iterator[I[(DataL, DataR)]] = ???

  override def get(key: I[(KeyL, KeyR)]): Option[I[(DataL, DataR)]] = {
    (dataMapL.get(key.a._1), dataMapR.get(key.a._2)) match{
      case (Some(x), Some(y)) => Some(I(x,y))
      case (Some(x), None) => Some(I(x, ???))
      case (None, Some(y)) => Some(I(???, y))
      case (None, None) => None
    }
  }

  override def all(): Seq[I[(DataL, DataR)]] = ???

  override def contains(key: I[(KeyL, KeyR)]): Boolean = get(key) match{
    case None => false
    case _ => true
  }

  override def extract(): Seq[I[(DataL, DataR)]] = ???

  override def size(): Int = dataMapL.size*dataMapR.size
}
