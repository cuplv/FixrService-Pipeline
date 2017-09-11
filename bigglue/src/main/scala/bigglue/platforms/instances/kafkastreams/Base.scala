package bigglue.platforms.instances.kafkastreams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, ValueMapper}
import bigglue.computations.{Computation, Mapper}
import bigglue.configurations.{PipeConfig, PlatformBuilder}
import bigglue.data.Identifiable
import bigglue.data.serializers.BasicSerializer
import bigglue.exceptions.{NotInitializedException, NotSupportedPipelineConfigException}
import bigglue.platforms.UnaryPlatform
import bigglue.store.DataStore
import bigglue.store.instances.kafka.{KafkaConfig, KafkaDataMap, KafkaQueue}

import scala.util.Random

import scala.collection.JavaConverters._


/**
  * Created by edmundlam on 9/6/17.
  */

object KafkaStreamPlatform {

  final val NAME: String = "kafka-stream-platform"

  def extractKafkaTopic[Data <: Identifiable[Data]](store: DataStore[Data]): String = {
    if (store.isInstanceOf[KafkaQueue[Data]]) return store.asInstanceOf[KafkaQueue[Data]].getTopic
    if (store.isInstanceOf[KafkaDataMap[_,Data]]) return store.asInstanceOf[KafkaDataMap[_,Data]].getTopic
    return null
  }

}

abstract class KafkaStreamPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = KafkaStreamPlatform.NAME + s"-unary-${Random.nextInt(99999)}") extends UnaryPlatform[Input,Output] {

  var inputTopicOpt: Option[String]  = None
  var outputTopicOpt: Option[String] = None
  var streamBuilderOpt: Option[KStreamBuilder] = None
  var streamOpt: Option[KafkaStreams] = None

  def getInputTopic: String = inputTopicOpt match {
    case Some(inputTopic) => inputTopic
    case None => throw new NotInitializedException("KafkaStreamPlatform", "getInputTopic", None)
  }

  def getOutputTopic: String = outputTopicOpt match {
    case Some(outputTopic) => outputTopic
    case None => throw new NotInitializedException("KafkaStreamPlatform", "getOutputTopic", None)
  }

  def getStreamBuilder: KStreamBuilder = streamBuilderOpt match {
    case Some(streamBuilder) => streamBuilder
    case None => throw new NotInitializedException("KafkaStreamPlatform", "getStreamBuilder", None)
  }

  def getStream: KafkaStreams = streamOpt match {
    case Some(stream) => stream
    case None => throw new NotInitializedException("KafkaStreamPlatform", "getStream", None)
  }

  def start(computation: Computation): Unit

  override def check(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output]): Unit = {
    if(!inputMap.isInstanceOf[KafkaQueue[Input]] && !inputMap.isInstanceOf[KafkaDataMap[_,Input]]) {
       throw new NotSupportedPipelineConfigException(s"Kafka platform cannot be associated with input datastore: ${inputMap.getClass.getName}", None)
    }
    if(!outputMap.isInstanceOf[KafkaQueue[Output]] && !outputMap.isInstanceOf[KafkaDataMap[_,Output]]) {
      throw new NotSupportedPipelineConfigException(s"Kafka platform cannot be associated with output datastore: ${outputMap.getClass.getName}", None)
    }
  }

  override def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
     super.init(conf, inputMap, outputMap, builder)
     val inputTopic  = KafkaStreamPlatform.extractKafkaTopic(inputMap)
     val outputTopic = KafkaStreamPlatform.extractKafkaTopic(outputMap)
     val streamBuilder: KStreamBuilder = new KStreamBuilder()
     val kafkaConf = KafkaConfig.resolveKafkaConfig(name, conf)
     val stream = new KafkaStreams(streamBuilder, kafkaConf.streamProps)
     inputTopicOpt  = Some(inputTopic)
     outputTopicOpt = Some(outputTopic)
     streamBuilderOpt = Some(streamBuilder)
     streamOpt = Some(stream)
     computationOpt match {
       case Some(computation) => start(computation)
       case None =>
     }
  }

  override def wake(): Unit = { }

  override def terminate(): Unit = streamOpt match {
    case Some(stream) => stream.close()
    case None =>
  }

}

class KafkaValueMapper[Input <: Identifiable[Input],Output <: Identifiable[Output]]
     (inputSerializer: BasicSerializer[Input], outputSerializer: BasicSerializer[Output], mapper: Mapper[Input,Output]) extends ValueMapper[String,java.util.List[String]] {

  override def apply(value: String): java.util.List[String] = {
    val input = inputSerializer.deserialize(value)
    mapper.tryCompute(input) match {
      case Some(outputs) => outputs.map( outputSerializer.serialize(_)).asJava
      case None => List.empty[String].asJava
    }
  }

}

class KafkaStreamMapperPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = KafkaStreamPlatform.NAME + s"-unary-${Random.nextInt(99999)}") extends KafkaStreamPlatform[Input,Output] {

  override def start(computation: Computation): Unit = computation match {
    case mapper: Mapper[Input,Output] => {
       val inputStream: KStream[String,String] = getStreamBuilder.stream(getInputTopic)
       val mappedStream = inputStream.flatMapValues( new KafkaValueMapper[Input,Output](???,???,???) )
       mappedStream.to(Serdes.String, Serdes.String, getOutputTopic)
    }
    case _ => ???
  }

}
