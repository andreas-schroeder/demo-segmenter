package driver
import java.time.Instant

import common._
import events._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.logging.log4j.{LogManager, Logger}
import sessions.SessionKey
import shapeless._

import scala.util.{Random, Try}

object Driver {

  def main(args: Array[String]): Unit = withConfig { config =>
    new Driver(config).start()
  }
}

class Driver(config: AppConfig) {

  val log: Logger = LogManager.getLogger()

  val settings: Map[String, String] = {
    import org.apache.kafka.clients.producer.ProducerConfig._
    Map(
      CLIENT_ID_CONFIG                      -> "driver",
      BOOTSTRAP_SERVERS_CONFIG              -> config.kafka.bootstrapServers,
      ENABLE_IDEMPOTENCE_CONFIG             -> "true",
      RETRIES_CONFIG                        -> Int.MaxValue.toString,
      MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> "5",
      ACKS_CONFIG                           -> "all",
      COMPRESSION_TYPE_CONFIG               -> "lz4"
    )
  }

  val keys: Seq[Device]    = (1 to 10).map(i => Device(s"device-$i"))
  val metrics: Seq[String] = Seq("pressure", "temperature", "wind", "humidity")

  val registry: String = config.kafka.schemaRegistry

  val keySerializer: Serializer[Device]                = Serdes.device(registry).serializer()
  val valueSerde: Serde[Envelope]                      = Serdes.envelope(registry)
  val sessionKeyDeserializer: Deserializer[SessionKey] = Serdes.sessionKey(registry).deserializer()

  def start(): Unit = {
    val producer = new KafkaProducer[Device, Envelope](settings.asProps, keySerializer, valueSerde.serializer())
    while (!Thread.currentThread.isInterrupted) {
      val key = keys(Random.nextInt(keys.length))
      val event = Random.nextInt(4) match {
        case 0     => Coproduct[Event](DeviceWokeUp())
        case 1 | 2 => Coproduct[Event](DataReceived(metrics(Random.nextInt(metrics.length)), Random.nextDouble()))
        case 3     => Coproduct[Event](AllDataReceived())
      }
      val attempt = Try {
        val time = Instant.now
        val callback: Callback = (m: RecordMetadata, ex: Exception) =>
          if (ex == null)
            println(f"${time.shortFormat} ${m.topic}-${m.partition}@${m.offset}%04d [${key.id}] ${event.fold(unlift)}")
          else
            log.warn(s"${ex.getClass.getSimpleName}: ${ex.getMessage}")

        producer.send(new ProducerRecord("events", key, Envelope(event, time)), callback)
      }
      attempt.failed.foreach(ex => log.warn(s"${ex.getClass.getSimpleName}: ${ex.getMessage}"))

      if (config.mode == Demo) Thread.sleep(300)
    }
    producer.close()
  }
}
