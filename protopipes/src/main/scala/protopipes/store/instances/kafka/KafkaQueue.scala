package protopipes.store.instances.kafka

import java.util
import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import protopipes.configurations.PipeConfig
import protopipes.connectors.Connector
import protopipes.store.DataQueue
import protopipes.store.instances.InMemDataQueue

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by edmundlam on 8/19/17.
  */
abstract class KafkaQueue[Data] extends DataQueue[Data] {

  // var buffer: Seq[Data] = Seq.empty[Data]
  // var consumerOpt: Option[KafkaConsumer[String,String]] = None

  var iteratorOpt: Option[KafkaQueueIterator[Data]] = None
  var producerOpt: Option[KafkaProducer[String,String]] = None
  var topicOpt: Option[String] = None
  var consPropsOpt: Option[Properties] = None
  var prodPropsOpt: Option[Properties] = None

  def toStr(data: Data): String
  def fromStr(str: String): Data

  def topic(topicName: String): KafkaQueue[Data] = {
    topicOpt = Some(topicName)
    this
  }

  override def init(conf: PipeConfig): Unit = {
    topic(name)

    val consProps: Properties = new Properties()
    consProps.put("bootstrap.servers", "localhost:9092")
    consProps.put("group.id", "test3")
    consProps.put("enable.auto.commit", "false")
    consProps.put("auto.commit.interval.ms", "1000")
    consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // val consumer = new KafkaConsumer[String,String](props)
    // consumer.subscribe(util.Arrays.asList(topicOpt.get))
    // consumerOpt = Some( consumer )

    val iterator = new KafkaQueueIterator[Data](this, topicOpt.get, consProps)
    iteratorOpt = Some( iterator )
    consPropsOpt = Some( consProps )

    import org.apache.kafka.clients.producer.KafkaProducer
    import org.apache.kafka.clients.producer.Producer
    val prodProps = new Properties()
    prodProps.put("bootstrap.servers", "localhost:9092")
    prodProps.put("acks", "all")
    prodProps.put("retries", "0")
    prodProps.put("batch.size", "16384")
    prodProps.put("linger.ms", "1")
    prodProps.put("buffer.memory", "33554432")
    prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](prodProps)
    producerOpt = Some(producer)
    prodPropsOpt = Some(prodProps)
  }

  override def put_(data: Seq[Data]): Unit = {
    val producer = producerOpt.get
    val topic = topicOpt.get
    data foreach {
      d => producer.send(new ProducerRecord[String, String](topic, toStr(d), toStr(d)))
    }
    iteratorOpt.get.pollForMore()
  }

  override def dequeue(): Option[Data] = iteratorOpt.get.dequeue()

  override def iterator(): Iterator[Data] = new KafkaQueueIterator[Data](this, topicOpt.get, consPropsOpt.get)

  override def all(): Seq[Data] = iteratorOpt.get.innerQueue.all()

  override def extract(): Seq[Data] = iteratorOpt.get.innerQueue.extract()

  override def size(): Int = iteratorOpt.get.size

  override def registerConnector(connector: Connector[Data]): Unit = {
    iteratorOpt.get.innerQueue.registerConnector(connector)
  }

}

class KafkaQueueIterator[Data](kafkaQueue: KafkaQueue[Data], topic: String, props: Properties) extends Iterator[Data] {

  val innerQueue: DataQueue[Data] = new InMemDataQueue[Data]
  val consumer: KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)
  consumer.subscribe(util.Arrays.asList(topic))

  def pollForMore(tries: Int = 2, timeout: Int = 100): Boolean = {
    (0 to tries) foreach {
      _ => for (record <- consumer.poll(timeout)) {
        innerQueue.put( kafkaQueue.fromStr(record.value()) )
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

}