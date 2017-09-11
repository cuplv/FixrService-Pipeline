package bigglue.store

import bigglue.configurations.{ConfigBuildsDataStore, Constant, DataStoreBuilder, PipeConfig}
import bigglue.data.I
import bigglue.exceptions.NotInitializedException
import bigglue.store.instances.InMemDataQueue

/**
  * Created by edmundlam on 8/30/17.
  */
abstract class DataStream[StreamData] extends Iterator[StreamData] with ConfigBuildsDataStore {

  var bufferOpt: Option[DataQueue[StreamData]] = None

  def init(conf: PipeConfig): Unit = {
    val builder = constructBuilder(conf)
    val buffer = builder.mkAuxStore[DataQueue[StreamData]](Constant.DATASTREAMBUFFER) match {
      case Some(buffer) => buffer
      case None => new InMemDataQueue[StreamData]
    }
    bufferOpt = Some(buffer)
  }

  def getBuffer(): DataQueue[StreamData] = bufferOpt match {
    case Some(buffer) => buffer
    case None => throw new NotInitializedException("DataStream", "getBuffer", None)
  }

  def pollForMore(tries: Int = 2, timeout: Int = 100): Boolean

  def close(): Unit

  override def size(): Int = getBuffer().size()

  override def hasNext: Boolean = getBuffer().size() > 0

  override def next(): StreamData = getBuffer().dequeue().get

}

abstract class DataStreamAdapter[Input,Output](inputStream: DataStream[Input]) extends DataStream[Output] {

  def toOutput(input: Input): Output

  override def pollForMore(tries: Int, timeout: Int): Boolean = inputStream.pollForMore(tries, timeout)

  override def close(): Unit = inputStream.close()

  override def size(): Int = inputStream.size()

  override def hasNext: Boolean = inputStream.hasNext

  override def next(): Output = toOutput( inputStream.next() )

}

class DropLeftDataStreamAdaptor[Output](inputStream: DataStream[(_,Output)]) extends DataStreamAdapter[(_,Output),Output](inputStream) {

  override def toOutput(input: (_, Output)): Output = input._2

}