package bigglue.pipes

import bigglue.computations.{Mapper, PairwiseComposer, Reducer}
import bigglue.configurations.PipeConfig
import bigglue.data.{I, Identifiable}
import bigglue.exceptions.{MalformedPipelineConfigException, NotInitializedException, UnexpectedPipelineException}
import bigglue.pipes.Implicits.PairPipes
import bigglue.store.instances.{BothDataStore, InMemIdDataMap}
import bigglue.store.{DataMap, IdDataMap, TupleDataMap, TupleDataStore}

import scala.collection.parallel.ParIterable
// import bigpipes.pipes.Implicits.PairPipes
import bigglue.store.DataStore
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */



abstract class Pipe[Head <: Identifiable[Head], End <: Identifiable[End]] {

  def check(conf: PipeConfig): Unit

  def init(conf: PipeConfig): Unit

  def head(): DataStore[Head]

  def end(): DataStore[End]

  def terminate(): Unit

  def :--[Next <: Identifiable[Next], Mid <: Identifiable[Mid]](headMapper: PartialMapperPipe[End,Mid,Next]): Pipe[Head,Next] = {
    MapperPipe(this, headMapper.mapper, headMapper.p)
  }

  def :-+[Next <: Identifiable[Next], Mid <: Identifiable[Mid]](headReducer: PartialReducerPipe[End,Mid,Next]): Pipe[Head,Next] = {
    ReducerPipe(this, headReducer.reducer, headReducer.p)
  }

  def :<[Next <: Identifiable[Next]](parPipes: PartialPipe[End,Next]): Pipe[Head,Next] = {
    JunctionPipe(this, parPipes)
  }

  def ||[OHead <: Identifiable[OHead], OEnd <: Identifiable[OEnd]](o: Pipe[OHead,OEnd]): PairPipes[Head,OHead,End,OEnd] = {
    new PairPipes[Head,OHead,End,OEnd](this, o)
  }

}

case class St[Data <: Identifiable[Data]](store: DataStore[Data]) extends Pipe[Data,Data] {
  override def toString: String = store.displayName
  override def check(conf: PipeConfig): Unit = store.checkConfig(conf)
  override def init(conf: PipeConfig): Unit = store.init(conf)
  override def head(): DataStore[Data] = store
  override def end(): DataStore[Data] = store
  override def terminate(): Unit = store.terminate()

}

object Implicits {
  implicit class DataNode[Data <: Identifiable[Data]](map: DataStore[Data]) extends Pipe[Data,Data] {
    override def toString: String = map.displayName
    override def check(conf: PipeConfig): Unit = map.checkConfig(conf)
    override def init(conf: PipeConfig): Unit = map.init(conf)
    override def head(): DataStore[Data] = map
    override def end(): DataStore[Data] = map
    override def terminate(): Unit = map.terminate()
  }

  /*
  implicit class DataNode2[Data <: Identifiable[Data]](map: InMemIdDataMap[Data]) extends Pipe[Data,Data] {
    override def toString: String = map.displayName
    override def init(conf: Config): Unit = { }
    override def head(): DataStore[Data] = map
    override def end(): DataStore[Data] = map
    override def terminate(): Unit = { }
  } */

  implicit class PairPipes[HeadL <: Identifiable[HeadL],HeadR <: Identifiable[HeadR],EndL <: Identifiable[EndL],EndR <: Identifiable[EndR]]
  (ps: (Pipe[HeadL,EndL],Pipe[HeadR,EndR])) {
    def :-*[Output <: Identifiable[Output],End <: Identifiable[End]](parComp: PartialComposerPipe[EndL,EndR,Output,End])
    : CompositionPipe[HeadL,HeadR,EndL,EndR,Output,End] = {
      CompositionPipe(ps._1, parComp.composer, ps._2, parComp.p)
    }
  }

  implicit class TuplePipe[DataL <: Identifiable[DataL], DataR <: Identifiable[DataR], EndL <: Identifiable[EndL], EndR <: Identifiable[EndR]]
  (pipes: (Pipe[DataL, EndL], Pipe[DataR, EndR])) extends Pipe[I[(DataL, DataR)], I[(EndL, EndR)]]{
    override def toString(): String = s"TuplePipe($pipes)"

    private val headStore = (pipes._1.head(), pipes._2.head()) match{
      case (x1: DataMap[_, DataL], x2: DataMap[_, DataR]) => new TupleDataMap(x1, x2)
      case (x1, x2) => new TupleDataStore(x1, x2)
    }

    private val endStore = (pipes._1.end(), pipes._2.end()) match {
      case (x1: DataMap[_, EndL], x2: DataMap[_, EndR]) => new TupleDataMap(x1, x2)
      case (x1, x2) => new TupleDataStore(x1, x2)
    }

    override def head(): DataStore[I[(DataL, DataR)]] = headStore
    override def end(): DataStore[I[(EndL, EndR)]] = endStore
    override def init(conf: PipeConfig): Unit = {
      pipes._1.init(conf)
      pipes._2.init(conf)
    }
    override def check(conf: PipeConfig): Unit = {
      pipes._1.check(conf)
      pipes._2.check(conf)
    }
    override def terminate(): Unit = {
      pipes._1.terminate()
      pipes._2.terminate()
    }
  }
}

case class MapperPipe[Head <: Identifiable[Head], Input <: Identifiable[Input], Output <: Identifiable[Output], End <: Identifiable[End]]
(p1: Pipe[Head,Input], mapper: Mapper[Input,Output], p2: Pipe[Output,End]) extends Pipe[Head,End] {

  /*
  var mapperOpt: Option[Mapper[Input,Output]] = None

  def getMapper: Mapper[Input,Output] = mapperOpt match {
    case Some(mapper) => mapper
    case None => {
      val mapper = new Mapper[Input,Output](fmap)
      mapperOpt = Some(mapper)
      mapper
    }
  } */

  override def check(conf: PipeConfig): Unit = {
    p1.check(conf)
    mapper.check(conf, p1.end(), p2.head())
    p2.check(conf)
  }

  override def init(conf: PipeConfig): Unit = {
    p1.init(conf)
    mapper.init(conf, p1.end(), p2.head())
    p2.init(conf)
  }

  override def head(): DataStore[Head] = p1.head()

  override def end(): DataStore[End] = p2.end()

  override def terminate(): Unit = {
    p1.terminate()
    mapper.terminate()
    p2.terminate()
  }

}

case class ReducerPipe[Head <: Identifiable[Head], Input <: Identifiable[Input], Output <: Identifiable[Output], End <: Identifiable[End]]
(p1: Pipe[Head,Input], reducer: Reducer[Input,Output], p2: Pipe[Output,End]) extends Pipe[Head,End] {

  override def check(conf: PipeConfig): Unit = {
    p1.check(conf)
    reducer.check(conf, p1.end(), p2.head())
    p2.check(conf)
  }

  override def init(conf: PipeConfig): Unit = {
    p1.init(conf)
    reducer.init(conf, p1.end(), p2.head())
    p2.init(conf)
  }

  override def head(): DataStore[Head] = p1.head()

  override def end(): DataStore[End] = p2.end()

  override def terminate(): Unit = {
    p1.terminate()
    reducer.terminate()
    p2.terminate()
  }

}

case class CompositionPipe[HeadL <: Identifiable[HeadL], HeadR <: Identifiable[HeadR], InputL <: Identifiable[InputL], InputR <: Identifiable[InputR]
                          ,Output <: Identifiable[Output], End <: Identifiable[End]]
(p1: Pipe[HeadL,InputL], composer: PairwiseComposer[InputL,InputR,Output], p2: Pipe[HeadR,InputR], o: Pipe[Output,End]) extends Pipe[bigglue.data.Either[HeadL,HeadR],End] {

  override def check(conf: PipeConfig): Unit = {
    p1.check(conf)
    p2.check(conf)
    composer.check(conf, p1.end(), p2.end(), o.head())
    o.check(conf)
  }

  override def init(conf: PipeConfig): Unit = {
    p1.init(conf)
    p2.init(conf)
    composer.init(conf, p1.end(), p2.end(), o.head())
    o.init(conf)
  }

  override def head(): DataStore[bigglue.data.Either[HeadL,HeadR]] = BothDataStore(p1.head(),p2.head())

  override def end(): DataStore[End] = o.end()

  override def terminate(): Unit = {
    p1.terminate()
    p2.terminate()
    composer.terminate()
    o.terminate()
  }

}


case class PartialComposerPipe[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output], End <: Identifiable[End]]
(composer: PairwiseComposer[InputL,InputR,Output], p: Pipe[Output,End]) {

}


case class JunctionPipe[Head <: Identifiable[Head], Mid <: Identifiable[Mid], End <: Identifiable[End]](p1: Pipe[Head,Mid], p2: PartialPipe[Mid,End]) extends Pipe[Head,End] {

  override def check(conf: PipeConfig): Unit = {
    p1.check(conf)
    p2.check(conf, p1.end())
  }

  override def init(conf: PipeConfig): Unit = {
    p1.init(conf)
    p2.init(conf, p1.end())
  }

  override def head(): DataStore[Head] = p1.head()

  override def end(): DataStore[End] = p2.end()

  override def terminate(): Unit = {
    p1.terminate()
    p2.terminate()
  }

}

abstract class PartialPipe[Input <: Identifiable[Input], End <: Identifiable[End]] {

  def check(conf: PipeConfig, input: DataStore[Input]): Unit

  def init(conf: PipeConfig, input: DataStore[Input]): Unit

  def end(): DataStore[End]

  def terminate(): Unit

  def ~[Other <: Identifiable[Other]](other: PartialPipe[Input,Other]): PartialPipe[Input, bigglue.data.Either[End,Other]] = {
    ParallelPartialPipes(this, other)
  }

  def :--[Next <: Identifiable[Next],Mid <: Identifiable[Mid]](other: PartialMapperPipe[End,Mid,Next]): PartialPipe[Input,Next] = {
    SequencedPartialPipes(this, other)
  }

  def :-+[Next <: Identifiable[Next],Mid <: Identifiable[Mid]](other: PartialReducerPipe[End,Mid,Next]): PartialPipe[Input,Next] = {
    SequencedPartialPipes(this, other)
  }

}

case class PartialMapperPipe[Input <: Identifiable[Input], Output <: Identifiable[Output], End <: Identifiable[End]]
(mapper: Mapper[Input,Output], p: Pipe[Output,End]) extends PartialPipe[Input,End] {

  /*
  var mapperOpt: Option[Mapper[Input,Output]] = None

  def getMapper: Mapper[Input,Output] = mapperOpt match {
    case Some(mapper) => mapper
    case None => {
      val mapper = new Mapper[Input,Output](fmap)
      mapperOpt = Some(mapper)
      mapper
    }
  } */

  override def check(conf: PipeConfig, input: DataStore[Input]): Unit = {
    mapper.check(conf, input, p.head())
    p.check(conf)
  }

  override def init(conf: PipeConfig, input: DataStore[Input]): Unit = {
    mapper.init(conf, input, p.head())
    p.init(conf)
  }

  override def end(): DataStore[End] = p.end()

  override def terminate(): Unit = {
    mapper.terminate()
    p.terminate()
  }

}

case class PartialReducerPipe[Input <: Identifiable[Input], Output <: Identifiable[Output], End <: Identifiable[End]]
(reducer: Reducer[Input,Output], p: Pipe[Output,End]) extends PartialPipe[Input,End] {

  override def check(conf: PipeConfig, input: DataStore[Input]): Unit = {
    reducer.check(conf, input, p.head())
    p.check(conf)
  }

  override def init(conf: PipeConfig, input: DataStore[Input]): Unit = {
    reducer.init(conf, input, p.head())
    p.init(conf)
  }

  override def end(): DataStore[End] = p.end()

  override def terminate(): Unit = {
    reducer.terminate()
    p.terminate()
  }

}

case class SequencedPartialPipes[Input <: Identifiable[Input], Mid <: Identifiable[Mid], Output <: Identifiable[Output]]
     (pi: PartialPipe[Input,Mid], po: PartialPipe[Mid,Output]) extends PartialPipe[Input,Output] {

  override def check(conf: PipeConfig, input: DataStore[Input]): Unit = {
    pi.check(conf, input)
    po.check(conf, pi.end())
  }

  override def init(conf: PipeConfig, input: DataStore[Input]): Unit = {
    pi.init(conf, input)
    po.init(conf, pi.end())
  }

  override def end(): DataStore[Output] = po.end()

  override def terminate(): Unit = {
    pi.terminate()
    po.terminate()
  }

}


case class ParallelPartialPipes[Input <: Identifiable[Input], LEnd <: Identifiable[LEnd], REnd <: Identifiable[REnd]](p1: PartialPipe[Input,LEnd], p2: PartialPipe[Input,REnd]) extends PartialPipe[Input, bigglue.data.Either[LEnd,REnd]] {

  override def check(conf: PipeConfig, input: DataStore[Input]): Unit = {
    p1.check(conf, input)
    p2.check(conf, input)
  }

  override def init(conf: PipeConfig, input: DataStore[Input]): Unit = {
    p1.init(conf, input)
    p2.init(conf, input)
  }

  override def end(): DataStore[bigglue.data.Either[LEnd,REnd]] = {
    BothDataStore(p1.end(),p2.end())
  }

  override def terminate(): Unit = {
    p1.terminate()
    p2.terminate()
  }

}