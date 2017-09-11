package bigglue.store.instances.kafka

import java.util
import java.util.Properties
import java.util.function.Consumer

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import bigglue.configurations.PipeConfig
import bigglue.connectors.Connector
import bigglue.connectors.instances.{Adaptor, IgnoreKeyAdaptor}
import bigglue.data.Identifiable
import bigglue.data.serializers.BasicSerializer
import bigglue.exceptions.NotInitializedException
import bigglue.store.{DataQueue, DataStream, DropLeftDataStreamAdaptor}
import bigglue.store.instances.InMemDataQueue

import scala.collection.JavaConverters._
// import scala.collection.JavaConversions._

/**
  * Created by edmundlam on 8/19/17.
  */
abstract class KafkaQueue[Data <: Identifiable[Data]] extends DataQueue[Data] {

  var streamOpt: Option[KafkaStream[String, Data]] = None
  var producerOpt: Option[KafkaProducer[String,String]] = None
  var topicOpt: Option[String] = None
  var consPropsOpt: Option[Properties] = None
  var prodPropsOpt: Option[Properties] = None

  val serializer: BasicSerializer[Data]

  def getStream: KafkaStream[String, Data] = streamOpt match {
    case Some(stream) => stream
    case None => throw new NotInitializedException("KafkaQueue", "getStream", None)
  }

  def getProducer: KafkaProducer[String,String] = producerOpt match {
    case Some(producer) => producer
    case None => throw new NotInitializedException("KafkaQueue", "getProducer", None)
  }

  def getTopic: String = topicOpt match {
    case Some(topic) => topic
    case None => throw new NotInitializedException("KafkaQueue", "getTopic", None)
  }

  def getConsumerProps: Properties = consPropsOpt match {
    case Some(consProps) => consProps
    case None => throw new NotInitializedException("KafkaQueue", "getConsumerProps", None)
  }

  def getProducerProps: Properties = prodPropsOpt match {
    case Some(prodProps) => prodProps
    case None => throw new NotInitializedException("KafkaQueue", "getProducerProps", None)
  }

  def topic(topicName: String): KafkaQueue[Data] = {
    topicOpt = Some(topicName)
    this
  }

  override def init(conf: PipeConfig): Unit = {
    topicOpt match {
      case None => topic(name)
      case _ =>
    }
    val kafkaConf = KafkaConfig.resolveKafkaConfig(name, conf)

    val iterator = new KafkaStream[String, Data](serializer, topicOpt.get, kafkaConf.consumerProps)
    iterator.init(conf)

    streamOpt = Some( iterator )
    consPropsOpt = Some( kafkaConf.consumerProps )

    val producer = new KafkaProducer[String, String](kafkaConf.producerProps)
    producerOpt = Some(producer)
    prodPropsOpt = Some(kafkaConf.producerProps)
  }

  override def put_(data: Seq[Data]): Unit = {
    val producer = getProducer
    val topic = getTopic
    data foreach {
      d => producer.send(new ProducerRecord[String, String](topic, serializer.serialize(d), serializer.serialize(d)))
    }
    getStream.pollForMore()
  }

  override def dequeue(): Option[Data] = if (getStream.size() > 0) Some(getStream.next()._2) else None

  override def iterator(): Iterator[Data] = new DropLeftDataStreamAdaptor[Data](
    new KafkaStream[String, Data](serializer, getTopic, getConsumerProps).asInstanceOf[DataStream[(_,Data)]] )

  override def all(): Seq[Data] = getStream.getBuffer.all().map( _._2 )

  override def extract(): Seq[Data] = getStream.getBuffer.extract().map( _._2 )

  override def size(): Int = getStream.size

  override def registerConnector(connector: Connector[Data]): Unit = {
    getStream.getBuffer.registerConnector( new IgnoreKeyAdaptor[String,Data](connector,null) )
  }

  def pollForMore(tries: Int = 2, timeout: Int = 100): Boolean = getStream.pollForMore(tries, timeout)

}
/*
class KafkaQueueIterator[Data <: Identifiable[Data]](serializer: BasicSerializer[Data], topic: String, props: Properties) extends Iterator[Data] {

  val innerQueue: DataQueue[Data] = new InMemDataQueue[Data]
  val consumer: KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)
  consumer.subscribe(util.Arrays.asList(topic))

  def pollForMore(tries: Int = 2, timeout: Int = 100): Boolean = {
    (0 to tries) foreach {
      _ =>  {
        val records = consumer.poll(timeout).iterator()
        while(records.hasNext) {
          val record = records.next()
          innerQueue.put( serializer.deserialize(record.value()) )
        }
        /*
        for (record <- consumer.poll(timeout)) {
          innerQueue.put( serializer.deserialize(record.value()) )
        } */
      }
    }
    innerQueue.size > 0
  }

  override def size: Int = {
    if (innerQueue.size > 0) innerQueue.size else {
      pollForMore()
      innerQueue.size()
    }
  }

  def dequeue(): Option[Data] = innerQueue.dequeue()

  override def hasNext: Boolean = {
    if (innerQueue.size > 0) true else pollForMore()
  }

  override def next(): Data = {
    dequeue match {
      case Some(data) => data
      case None => {
        pollForMore()
        dequeue.get
      }
    }
  }

}*/