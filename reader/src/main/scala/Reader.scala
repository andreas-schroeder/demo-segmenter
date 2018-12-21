import java.time.Duration

import common._
import events._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.logging.log4j.{LogManager, Logger}
import sessions.SessionKey

import scala.collection.JavaConverters._

object Reader {

  def main(args: Array[String]): Unit = withConfig { config =>
    new Reader(config).start()
  }
}

class Reader(config: AppConfig) {

  val log: Logger = LogManager.getLogger()

  val settings: Map[String, String] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig._

    Map(
      CLIENT_ID_CONFIG         -> "reader",
      GROUP_ID_CONFIG          -> "reader",
      BOOTSTRAP_SERVERS_CONFIG -> config.kafka.bootstrapServers,
    )
  }

  val registry: String = config.kafka.schemaRegistry

  val valueDeserializer: Deserializer[Envelope]        = Serdes.envelope(registry).deserializer()
  val sessionKeyDeserializer: Deserializer[SessionKey] = Serdes.sessionKey(registry).deserializer()

  def start(): Unit = {
    val consumer =
      new KafkaConsumer[SessionKey, Envelope](settings.asProps, sessionKeyDeserializer, valueDeserializer)
    consumer.subscribe(Seq("session-events").asJava)
    while (!Thread.currentThread().isInterrupted) {
      consumer
        .poll(Duration.ofSeconds(1))
        .forEach(r =>
          println(
            f"${r.value.timestamp.shortFormat} ${r.topic}-${r.partition}@${r.offset}%04d [${r.key.device.id}:${r.key.sessionId}] ${r.value.event
              .fold(unlift)}"))
    }
    consumer.close()
  }
}
