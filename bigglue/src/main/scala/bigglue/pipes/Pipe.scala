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


/**
  * This is the superclass to [[MapperPipe]] and [[ReducerPipe]].
  * See either one of those for further details into what the pipelines end up doing.
  * @tparam Head
  * @tparam End
  */
abstract class Pipe[Head <: Identifiable[Head], End <: Identifiable[End]] {

  /**
    * This is called within the example with pipe.check(conf).
    * In basic terms, this checks to see whether the pipeline that we have created is valid.
    * @param conf The configuration file that we are checking with.
    */
  def check(conf: PipeConfig): Unit

  /**
    * This is called with the example with pipe.init(conf).
    * This initializes the pipeline and all of the parts within it.
    * @param conf The configuration file that we are initializing with. This ideally is the configuration file
    *             that is being used to check the pipeline.
    */
  def init(conf: PipeConfig): Unit

  /**
    * This starts/restarts the pipeline.
    */
  def run(): Unit = ()

  def head(): DataStore[Head]

  def end(): DataStore[End]

  def terminate(): Unit

  /**
    * This connects the [[Mapper]] to the input store, connecting it completely.
    * This is expected to be called after a [[bigglue.pipes.Implicits.DataNode]],
    * and can be seen after a and b.
    * @param headMapper The mapper to connect to the input store.
    * @tparam Next The type at the end of the new pipeline.
    * @tparam Mid The type that the mapper ends up outputting given the input [[End]].
    * @return A connected [[MapperPipe]] that connects this pipeline to the mapper.
    */
  def :--[Next <: Identifiable[Next], Mid <: Identifiable[Mid]](headMapper: PartialMapperPipe[End,Mid,Next]): Pipe[Head,Next] = {
    MapperPipe(this, headMapper.mapper, headMapper.p)
  }

  /**
    * This connects the [[Reducer]] to the input store, connecting it completely.
    * This is expected to be called after a [[bigglue.pipes.Implicits.DataNode]],
    * and can be seen after a and b.
    * @param headReducer The reducer to connect to the input store.
    * @tparam Next The type at the end of the new pipeline.
    * @tparam Mid The type that the mapper ends up outputting given the input [[End]].
    * @return A connected [[ReducerPipe]] that connects this pipeline to the reducer.
    */
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

  /**
    * This is a wrapper for data pipelines that make them act like pipes.
    * In the example, a, b, c, and d are converted implicitly to DataNodes when calling a:--AA-->b:--BB-->c:-+CC+->d.
    * @param map The Data Store that's being converted into a pipe.
    * @tparam Data The Type of the Data Store; In this case, it's I[Int] for a, b, and c, and [[bigglue.examples.Counter]] for d.
    */
  implicit class DataNode[Data <: Identifiable[Data]](map: DataStore[Data]) extends Pipe[Data,Data] {
    override def toString: String = map.displayName
    override def check(conf: PipeConfig): Unit = map.checkConfig(conf)

    /**
      * This initializes the DataStore [[map]] by calling [[map.init]](conf)
      * In this example, we would call [[bigglue.store.instances.solr.SolrDataMap.init]](conf)
      * @param conf The configuration file to initialize
      */
    override def init(conf: PipeConfig): Unit = map.init(conf)

    /**
      * This returns the data store hidden in the pipeline
      * @return [[map]] since this is both the head and end of this pipeline.
      */
    override def head(): DataStore[Data] = map

    /**
      * This returns the data store hidden in the pipeline
      * @return [[map]] since this is both the head and end of this pipeline.
      */
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
    override def run(): Unit = {
      pipes._1.run()
      pipes._2.run()
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

/**
  * This is created with the call a:--AA-->b (and b:--BB-->c, but we'll focus on the former) within the example.
  * In the case of a:--AA-->b, this is a pipeline that goes from Data Store a, computed by the mapper AA,
  * and then goes down into Data Store b to be sent further down the pipeline.
  * @param p1     An Input Data Pipe; [[bigglue.pipes.Implicits.DataNode]](a) in the example.
  *               In the case of b:--BB-->c, it would be the [[MapperPipe]](a:--AA-->b)
  * @param mapper The computation ([[Mapper]]) of which the inputs gets computed by. AA in the example.
  * @param p2     An Output Data Pipe; [[bigglue.pipes.Implicits.DataNode]](b) in this example.
  *               In the case of b:--BB-->c, it would be [[bigglue.pipes.Implicits.DataNode]](c).
  * @tparam Head The type of the data store that begins the pipeline; [[I]][Int] in this case.
  * @tparam Input The type of the data store that brings in input to this part of the pipeline; [[I]][Int] in this case.
  * @tparam Output The type of the data store that brings in output to this part of the pipeline; [[I]][Int] in this case.
  * @tparam End The type of the data store that shows up at the end of the pipeline; [[I]][Int] in this case.
  */
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

  /**
    * This is called within the example with pipe.check(conf).
    * In basic terms, this checks to see whether the pipeline that we have created is valid.
    * @param conf The configuration file that we are checking with.
    */
  override def check(conf: PipeConfig): Unit = {
    p1.check(conf)
    mapper.check(conf, p1.end(), p2.head())
    p2.check(conf)
  }

  /**
    * This is called with the example with pipe.init(conf).
    * This calls [[mapper.init]] with conf, and the data stores before and after the mapper computation.
    * These are called by [[p1.end]] and [[p2.end]]
    * This also moves the init call along the pipeline, sending it to the parts of the pipeline its connected to.
    * @param conf The configuration file that we are initializing with. This ideally is the configuration file
    *             that is being used to check the pipeline.
    */
  override def init(conf: PipeConfig): Unit = {
    p1.init(conf)
    mapper.init(conf, p1.end(), p2.head())
    p2.init(conf)
  }

  /**
    * This is called within the example pipe.run.
    * This calls [[mapper.persist]], which will check to see what data to send down the pipeline (again?).
    * Then, it will call [[p1.run]] and [[p2.run]], moving the run call along the pipeline.
    */
  override def run(): Unit = {
    p1.run()
    mapper.persist()
    p2.run()
  }

  /**
    * This gives you the data store at the start of the pipeline.
    * @return The Data Store inside the DataNode at the start of the pipeline.
    *         This is the same as [[p1.head]], as that's the part of the pipeline before the Mapper section.
    *         In this example, it would be a.
    */
  override def head(): DataStore[Head] = p1.head()

  /**
    * This gives you the data store at the end of the pipeline.
    * @return The Data Store inside the DataNode at the end of the pipeline.
    *         This is the same as [[p2.end]], as that's the part of the pipeline after the Mapper section.
    *         In this example, this would be b for a:--AA-->b. or c for a:--AA-->b:--BB-->c.
    */
  override def end(): DataStore[End] = p2.end()

  /**
    * This ends the pipeline by terminating the mapper with [[mapper.terminate]]. Then, it moves the terminate
    * call throughout the pipeline by calling [[Pipe.terminate]] on both [[p1]] and [[p2]].
    */
  override def terminate(): Unit = {
    p1.terminate()
    mapper.terminate()
    p2.terminate()
  }

}

/**
  * In the example, this is created with the call c:-+CC+->d within the val pipe = a:--AA-->b:--BB-->c:-+CC+->d line.
  * With this example, this is a pipeline that goes from Data Store c, computed by the reducer CC,
  * and then goes down into Data Store d to be sent further down the pipeline.
  * @param p1 The part of the pipeline before the actual reducer section of the pipeline; In the example, it would be
  *           the [[MapperPipe]](a:--AA-->b:--BB-->c)
  * @param reducer The computation ([[Reducer]]) on which the inputs get computed by. CC in the example.
  * @param p2 The part of the pipeline after the actual reducer section of the pipeline; In the example, it would be
  *           [[bigglue.pipes.Implicits.DataNode]](d)
  * @tparam Head The data store type at the very beginning of the pipeline. In the example, it would be [[I]][Int]
  * @tparam Input The type of the Data Store that brings in input to this part of the pipeline. [[I]][Int] in this case.
  * @tparam Output The type of the Data Store that this part of the pipeline outputs to. [[bigglue.examples.Counter]] in this case.
  * @tparam End The type of the data store that shows up at the end of the pipeline; [[bigglue.examples.Counter]] in this case.
  */
case class ReducerPipe[Head <: Identifiable[Head], Input <: Identifiable[Input], Output <: Identifiable[Output], End <: Identifiable[End]]
(p1: Pipe[Head,Input], reducer: Reducer[Input,Output], p2: Pipe[Output,End]) extends Pipe[Head,End] {

  /**
    * This is called within the example with pipe.check(conf).
    * In basic terms, this checks to see whether the pipeline that we have created is valid.
    * @param conf The configuration file that we are checking with.
    */
  override def check(conf: PipeConfig): Unit = {
    p1.check(conf)
    reducer.check(conf, p1.end(), p2.head())
    p2.check(conf)
  }

  /**
    * This is called with the example with pipe.init(conf).
    * This calls [[reducer.init]] with conf, and the data stores before and after the mapper computation.
    * These are called by [[p1.end]] and [[p2.end]]
    * This also moves the init call along the pipeline, sending it to the parts of the pipeline its connected to.
    * @param conf The configuration file that we are initializing with. This ideally is the configuration file
    *             that is being used to check the pipeline.
    */
  override def init(conf: PipeConfig): Unit = {
    p1.init(conf)
    reducer.init(conf, p1.end(), p2.head())
    p2.init(conf)
  }

  /**
    * This is called within the example pipe.run.
    * This calls [[reducer.persist]], which will check to see what data to send down the pipeline (again?).
    * Then, it will call [[p1.run]] and [[p2.run]], moving the run call along the pipeline.
    */
  override def run(): Unit = {
    p1.run()
    reducer.persist()
    p2.run()
  }

  /**
    * This gives you the data store at the start of the pipeline.
    * @return The Data Store inside the DataNode at the start of the pipeline.
    *         This is the same as [[p1.head]], as that's the part of the pipeline before the Reducer section.
    *         In this example, it would be a.
    */
  override def head(): DataStore[Head] = p1.head()

  /**
    * This gives you the data store at the end of the pipeline.
    * @return The Data Store inside the DataNode at the end of the pipeline.
    *         This is the same as [[p2.end]], as that's the part of the pipeline after the Reducer section.
    *         In this example, this would be d.
    */
  override def end(): DataStore[End] = p2.end()

  /**
    * This ends the pipeline by terminating the reducer with [[reducer.terminate]]. Then, it moves the terminate
    * call throughout the pipeline by calling [[Pipe.terminate]] on both [[p1]] and [[p2]].
    */
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

  override def run(): Unit = {
    p1.run()
    p2.run()
    composer.persist()
    o.run()
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

  override def run(): Unit = {
    p1.run()
    p2.run()
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

  def run(): Unit = ()

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

  override def run(): Unit = {
    mapper.persist()
    p.run()
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

  override def run(): Unit = {
    reducer.run()
    p.run()
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

  override def run(): Unit = {
    pi.run()
    po.run()
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

  override def run(): Unit = {
    p1.run()
    p2.run()
  }

  override def end(): DataStore[bigglue.data.Either[LEnd,REnd]] = {
    BothDataStore(p1.end(),p2.end())
  }

  override def terminate(): Unit = {
    p1.terminate()
    p2.terminate()
  }

}