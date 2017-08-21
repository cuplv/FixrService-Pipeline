package protopipes.store.instances.kafka

import java.util
import java.util.Properties
import java.util.function.Consumer

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
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
    topicOpt match {
      case None => topic(name)
      case _ =>
    }
    /*
    val consProps: Properties = new Properties()
    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test3")
    consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    */
    val kafkaConf = KafkaConfig.resolveKafkaConfig(name, conf)

    // val consumer = new KafkaConsumer[String,String](props)
    // consumer.subscribe(util.Arrays.asList(topicOpt.get))
    // consumerOpt = Some( consumer )

    val iterator = new KafkaQueueIterator[Data](this, topicOpt.get, kafkaConf.consumerProps)
    iteratorOpt = Some( iterator )
    consPropsOpt = Some( kafkaConf.consumerProps )

    /*
    val prodProps = new Properties()
    prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prodProps.put(ProducerConfig.ACKS_CONFIG, "all")
    prodProps.put(ProducerConfig.RETRIES_CONFIG, "0")
    prodProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    prodProps.put(ProducerConfig.LINGER_MS_CONFIG, "1")
    prodProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")
    prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") */

    val producer = new KafkaProducer[String, String](kafkaConf.producerProps)
    producerOpt = Some(producer)
    prodPropsOpt = Some(kafkaConf.producerProps)
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