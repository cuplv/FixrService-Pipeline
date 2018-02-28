package bigglue.store.instances.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import bigglue.configurations.PipeConfig
import bigglue.connectors.Connector
import bigglue.connectors.instances.IgnoreKeyAdaptor
import bigglue.data.Identifiable
import bigglue.data.serializers.BasicSerializer
import bigglue.exceptions.{CallNotAllowException, NotInitializedException}
import bigglue.store.{DataMap, DataQueue, DataStream, DropLeftDataStreamAdaptor}
import bigglue.store.instances.{InMemDataMap, InMemDataQueue}

/**
  * Created by edmundlam on 8/23/17.
  */
abstract class KafkaDataMap[Key, Data <: Identifiable[Data]] extends DataMap[Key,Data] {

  var streamOpt: Option[KafkaStream[Key,Data]] = None
  var producerOpt: Option[KafkaProducer[Key,String]] = None
  var topicOpt: Option[String] = None
  var consPropsOpt: Option[Properties] = None
  var prodPropsOpt: Option[Properties] = None

  var mapOpt: Option[DataMap[Key,Data]] = Some( new InMemDataMap[Key,Data] )

  val serializer: BasicSerializer[Data]

  override val serializerOpt: Option[BasicSerializer[Data]] = Some(serializer)

  def getStream: KafkaStream[Key, Data] = streamOpt match {
    case Some(stream) => stream
    case None => throw new NotInitializedException("KafkaDataMap", "getStream", None)
  }

  def getProducer: KafkaProducer[Key,String] = producerOpt match {
    case Some(producer) => producer
    case None => throw new NotInitializedException("KafkaDataMap", "getProducer", None)
  }

  def getTopic: String = topicOpt match {
    case Some(topic) => topic
    case None => throw new NotInitializedException("KafkaDataMap", "getTopic", None)
  }

  def getConsumerProps: Properties = consPropsOpt match {
    case Some(consProps) => consProps
    case None => throw new NotInitializedException("KafkaDataMap", "getConsumerProps", None)
  }

  def getProducerProps: Properties = prodPropsOpt match {
    case Some(prodProps) => prodProps
    case None => throw new NotInitializedException("KafkaDataMap", "getProducerProps", None)
  }

  def getMap(): DataMap[Key,Data] = mapOpt match {
    case Some(map) => map
    case None => throw new NotInitializedException("KafkaDataMap", "getMap()", None)
  }

  def topic(topicName: String): KafkaDataMap[Key,Data] = {
    topicOpt = Some(topicName)
    this
  }

  override def init(conf: PipeConfig): Unit = {
    topicOpt match {
      case None => topic(name)
      case _ =>
    }
    val kafkaConf = KafkaConfig.resolveKafkaConfig(name, conf)

    val iterator = new KafkaStream[Key,Data](serializer, topicOpt.get, kafkaConf.consumerProps)
    iterator.init(conf)
    streamOpt  = Some( iterator )
    consPropsOpt = Some( kafkaConf.consumerProps )

    val producer = new KafkaProducer[Key, String](kafkaConf.producerProps)
    producerOpt  = Some(producer)
    prodPropsOpt = Some(kafkaConf.producerProps)

  }

  override def put_(data: Seq[Data]): Unit =
     new CallNotAllowException("\'put\' method for Data Map needs to specify input key", None)

  override def put_(key: Key, data: Data): Unit = {
    val producer = getProducer
    val topic = getTopic
    producer.send(new ProducerRecord[Key, String](topic, key, serializer.serialize(data)))

    // iteratorOpt.get.pollForMore()
  }

  override def get(key: Key): Option[Data] = getMap().get(key)

  override def size(): Int = getMap().size()

  override def all(): Seq[Data] = getMap().all()

  override def extract(): Seq[Data] = getMap().extract()

  override def contains(key: Key): Boolean = getMap().contains(key)

  override def remove(keys: Seq[Key]): Unit = getMap().remove(keys)

  override def remove(key: Key): Unit = getMap().remove(key)

  override def iterator(): Iterator[Data] = new DropLeftDataStreamAdaptor[Data](
    new KafkaStream[String, Data](serializer, getTopic, getConsumerProps).asInstanceOf[DataStream[(_,Data)]] )

  override def registerConnector(connector: Connector[Data]): Unit = getStream.getBuffer.registerConnector( new IgnoreKeyAdaptor[Key,Data](connector, null.asInstanceOf[Key]))

  def pollForMore(tries: Int = 2, timeout: Int = 100): Boolean = {
    getStream.pollForMore(tries, timeout)
  }

  def dequeue(): Option[(Key,Data)] = {
    val stream = getStream
    if (stream.hasNext) Some(stream.next)
    else None
  }

}


/*
/**
  * Kafka map iterator that reports 'deltas' of an underlying Kafka topic. Similar to the queue iterator, this incrementally fetches new
  * entries (and modifications to old ones) applied on to the underlying Kafka topic.
  *
  * @param serializer
  * @param topic
  * @param props
  * @tparam Key
  * @tparam Data
  */
class KafkaMapIterator[Key, Data <: Identifiable[Data]](serializer: BasicSerializer[Data], topic: String, props: Properties) extends Iterator[(Key,Data)] {

  val innerMap: DataMap[Key,Data] = new InMemDataMap[Key,Data]
  val innerQueue: DataQueue[(Key,Data)] = new InMemDataQueue[(Key,Data)]
  val consumer: KafkaConsumer[Key,String] = new KafkaConsumer[Key,String](props)
  consumer.subscribe(util.Arrays.asList(topic))

  def pollForMore(tries: Int = 2, timeout: Int = 100): Boolean = {
    (0 to tries) foreach {
      _ =>  {
        val records = consumer.poll(timeout).iterator()
        while(records.hasNext) {
          val record = records.next()
          val value = serializer.deserialize(record.value())
          innerQueue.put( (record.key,value) )
          innerMap.put(record.key,value)
        }
      }
    }
    innerQueue.size > 0
  }

  override def size: Int = {
    if (innerMap.size > 0) innerMap.size else {
      pollForMore()
      innerMap.size()
    }
  }

  def dequeue(): Option[(Key,Data)] = innerQueue.dequeue()

  def get(key: Key): Option[Data] = {
    innerMap.get(key) match {
      case Some(value) => Some(value)
      case None => {
        pollForMore()
        innerMap.get(key)
      }
    }
  }

  override def hasNext: Boolean = {
    if (innerQueue.size > 0) true else pollForMore()
  }

  override def next(): (Key,Data) = {
    dequeue match {
      case Some(data) => data
      case None => {
        pollForMore()
        dequeue.get
      }
    }
  }

} */