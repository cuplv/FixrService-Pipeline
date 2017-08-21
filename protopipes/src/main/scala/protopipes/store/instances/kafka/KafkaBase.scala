package protopipes.store.instances.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import protopipes.configurations.{Constant, PipeConfig, PropKey}

/**
  * Created by edmundlam on 8/20/17.
  */
object KafkaConfig {

  final val KAFKA: String = "kafka"
  final val CONSUMER: String = "consumer"
  final val PRODUCER: String = "producer"

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

  def resolveKafkaConfig(pipeLabel: String, config: PipeConfig): KafkaConfig = {
    var consProps = config.javaUtilsProps.get(PropKey(pipeLabel,CONSUMER)) match {
      case Some(props) => props
      case None => defaultConsumerConfig()
    }
    var prodProps = config.javaUtilsProps.get(PropKey(pipeLabel,PRODUCER)) match {
      case Some(props) => props
      case None => defaultProducerConfig()
    }
    val protoConf = PipeConfig.liftTypeSafeConfigToLabel(config.typeSafeConfig, pipeLabel).getConfig(Constant.PROTOPIPES)
    if (protoConf.hasPath(Constant.KAFKA)) {
      val tsConf = protoConf.getConfig(Constant.KAFKA)
      if (tsConf.hasPath(CONSUMER)) consProps = PipeConfig.resolveConfig(consProps, tsConf.getConfig(CONSUMER))
      if (tsConf.hasPath(PRODUCER)) prodProps = PipeConfig.resolveConfig(prodProps, tsConf.getConfig(PRODUCER))
    }
    KafkaConfig(consProps,prodProps)
  }

}

case class KafkaConfig(consumerProps: Properties, producerProps: Properties) {
}
