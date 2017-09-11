package bigglue.store.instances.kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import bigglue.configurations.{Constant, PipeConfig, PropKey}
import bigglue.data.Identifiable
import bigglue.data.serializers.BasicSerializer
import bigglue.store.DataStream

/**
  * Created by edmundlam on 8/20/17.
  */
object KafkaConfig {

  final val KAFKA: String = "kafka"
  final val CONSUMER: String = "consumer"
  final val PRODUCER: String = "producer"
  final val STREAM: String = "stream"

  def defaultConsumerConfig(): Properties = {
    val consProps: Properties = new Properties()
    consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test3")
    consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consProps
  }

  def defaultProducerConfig(): Properties = {
    val prodProps = new Properties()
    prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prodProps.put(ProducerConfig.ACKS_CONFIG, "all")
    prodProps.put(ProducerConfig.RETRIES_CONFIG, "0")
    prodProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    prodProps.put(ProducerConfig.LINGER_MS_CONFIG, "1")
    prodProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")
    prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prodProps
  }

  def defaultStreamConfig(): Properties = {
    val streamProps = new Properties()
    streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "default-application")
    streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    streamProps
  }

  def resolveKafkaConfig(pipeLabel: String, config: PipeConfig): KafkaConfig = {
    var consProps = config.javaUtilsProps.get(PropKey(pipeLabel,CONSUMER)) match {
      case Some(props) => props
      case None => defaultConsumerConfig()
    }
    var prodProps = config.javaUtilsProps.get(PropKey(pipeLabel,PRODUCER)) match {
      case Some(props) => props
      case None => defaultProducerConfig()
    }
    var streamProps = config.javaUtilsProps.get(PropKey(pipeLabel,STREAM)) match {
      case Some(props) => props
      case None => defaultStreamConfig()
    }
    val protoConf = PipeConfig.liftTypeSafeConfigToLabel(config.typeSafeConfig, pipeLabel).getConfig(Constant.BIGGLUE)
    if (protoConf.hasPath(Constant.KAFKA)) {
      val tsConf = protoConf.getConfig(Constant.KAFKA)
      if (tsConf.hasPath(CONSUMER)) consProps = PipeConfig.resolveConfig(consProps, tsConf.getConfig(CONSUMER))
      if (tsConf.hasPath(PRODUCER)) prodProps = PipeConfig.resolveConfig(prodProps, tsConf.getConfig(PRODUCER))
      if (tsConf.hasPath(STREAM)) streamProps = PipeConfig.resolveConfig(streamProps, tsConf.getConfig(STREAM))
    }
    KafkaConfig(consProps,prodProps,streamProps)
  }

}

case class KafkaConfig(consumerProps: Properties, producerProps: Properties, streamProps: Properties) {
}

class KafkaStream[Key,Data <: Identifiable[Data]]
       (dataSerializer: BasicSerializer[Data], topic: String, props: Properties) extends DataStream[(Key,Data)] {

  val consumer: KafkaConsumer[Key,String] = new KafkaConsumer[Key,String](props)
  consumer.subscribe(util.Arrays.asList(topic))

  override def pollForMore(tries: Int = 2, timeout: Int = 100): Boolean = {
    (0 to tries) foreach {
      _ =>  {
        val records = consumer.poll(timeout).iterator()
        while(records.hasNext) {
          val record = records.next()
          getBuffer().put( (record.key(), dataSerializer.deserialize(record.value())) )
        }
      }
    }
    getBuffer().size > 0
  }

  override def close(): Unit = consumer.close()

}