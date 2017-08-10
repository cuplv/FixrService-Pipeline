package protopipes.pipes

import protopipes.computations.Mapper
import protopipes.data.Identifiable
import protopipes.store.instances.InMemIdDataMap
import protopipes.store.{DataMap, IdDataMap}
// import bigpipes.pipes.Implicits.PairPipes
import protopipes.store.DataStore
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */



abstract class Pipe[Head, End <: Identifiable[End]] {

  def init(conf: Config): Unit

  def head(): DataStore[Head]

  def end(): DataStore[End]

  def terminate(): Unit

  def :--[Next <: Identifiable[Next], Mid](headMapper: PartialMapperPipe[End,Mid,Next]): Pipe[Head,Next] = {
    MapperPipe(this, headMapper.mapper, headMapper.p)
  }

  def :<[Next <: Identifiable[Next]](parPipes: PartialPipe[End,Next]): Pipe[Head,Next] = {
    JunctionPipe(this, parPipes)
  }
  /*
  def ||[OHead, OEnd](o: Pipe[OHead,OEnd]): PairPipes[Head,OHead,End,OEnd] = {
    new PairPipes[Head,OHead,End,OEnd](this, o)
  } */

}

case class St[Data <: Identifiable[Data]](store: DataStore[Data]) extends Pipe[Data,Data] {
  override def toString: String = store.displayName
  override def init(conf: Config): Unit = { }
  override def head(): DataStore[Data] = store
  override def end(): DataStore[Data] = store
  override def terminate(): Unit = { }

}

object Implicits {
  implicit class DataNode1[Data <: Identifiable[Data]](map: DataStore[Data]) extends Pipe[Data,Data] {
    override def toString: String = map.displayName
    override def init(conf: Config): Unit = { }
    override def head(): DataStore[Data] = map
    override def end(): DataStore[Data] = map
    override def terminate(): Unit = { }
  }

  implicit class DataNode2[Data <: Identifiable[Data]](map: InMemIdDataMap[Data]) extends Pipe[Data,Data] {
    override def toString: String = map.displayName
    override def init(conf: Config): Unit = { }
    override def head(): DataStore[Data] = map
    override def end(): DataStore[Data] = map
    override def terminate(): Unit = { }
  }
  /*
  implicit class PairPipes[HeadL,HeadR,EndL,EndR]
  (ps: (Pipe[HeadL,EndL],Pipe[HeadR,EndR])) {
    def :-*[Output,End](parComp: PartialComposerPipe[EndL,EndR,Output,End])
    : CompositionPipe[HeadL,HeadR,EndL,EndR,Output,End] = {
      CompositionPipe(ps._1, parComp.composer, ps._2, parComp.p)
    }
  }*/
}

case class MapperPipe[Head, Input <: Identifiable[Input], Output, End <: Identifiable[End]]
(p1: Pipe[Head,Input], mapper: Mapper[Input,Output], p2: Pipe[Output,End]) extends Pipe[Head,End] {

  override def init(conf: Config): Unit = {
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
/*
case class CompositionPipe[HeadL, HeadR, InputL, InputR, Output, End]
(p1: Pipe[HeadL,InputL], composer: PairwiseComposer[InputL,InputR,Output], p2: Pipe[HeadR,InputR], o: Pipe[Output,End]) extends Pipe[raisingBulls.data.Either[HeadL,HeadR],End] {

  override def init(conf: Config): Unit = {
    p1.init(conf)
    p2.init(conf)
    composer.init(conf, p1.end(), p2.end(), o.head())
    o.init(conf)
  }

  override def head(): DataStore[raisingBulls.data.Either[HeadL,HeadR]] = EitherDataMap(p1.head(),p2.head())

  override def end(): DataStore[End] = o.end()

  override def terminate(): Unit = {
    p1.terminate()
    p2.terminate()
    composer.terminate()
    o.terminate()
  }

} */

/*
case class PartialComposerPipe[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output], End <: Identifiable[End]]
(composer: PairwiseComposer[InputL,InputR,Output], p: Pipe[Output,End]) {

}
*/


case class JunctionPipe[Head, Mid <: Identifiable[Mid], End <: Identifiable[End]](p1: Pipe[Head,Mid], p2: PartialPipe[Mid,End]) extends Pipe[Head,End] {

  override def init(conf: Config): Unit = {
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

abstract class PartialPipe[Input, End] {

  def init(conf: Config, input: DataStore[Input]): Unit

  def end(): DataStore[End]

  def terminate(): Unit

  /*
  def ~[Other](other: PartialPipe[Input,Other]): PartialPipe[Input, raisingBulls.data.Either[End,Other]] = {
    ParallelPartialPipes(this, other)
  }*/

}

case class PartialMapperPipe[Input <: Identifiable[Input], Output, End <: Identifiable[End]](mapper: Mapper[Input,Output], p: Pipe[Output,End]) extends PartialPipe[Input,End] {

  override def init(conf: Config, input: DataStore[Input]): Unit = {
    mapper.init(conf, input, p.head())
    p.init(conf)
  }

  override def end(): DataStore[End] = p.end()

  override def terminate(): Unit = {
    mapper.terminate()
    p.terminate()
  }

}

/*
case class ParallelPartialPipes[Input, LEnd, REnd](p1: PartialPipe[Input,LEnd], p2: PartialPipe[Input,REnd]) extends PartialPipe[Input, raisingBulls.data.Either[LEnd,REnd]] {

  override def init(conf: Config, input: DataStore[Input]): Unit = {
    p1.init(conf, input)
    p2.init(conf, input)
  }

  override def end(): DataStore[raisingBulls.data.Either[LEnd,REnd]] = {
    EitherDataMap(p1.end(),p2.end())
  }

  override def terminate(): Unit = {
    p1.terminate()
    p2.terminate()
  }

} */