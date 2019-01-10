package segmenter

import common._
import events._
import org.apache.kafka.common.Metric
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.state.Stores
import sessions.{Session, SessionKey}

import scala.collection.JavaConverters._
import scala.util.Random

object Segmenter {

  def main(args: Array[String]): Unit = withConfig { config =>
    new Segmenter(config, new SegmenterAvroSerdes(config)).start()
  }
}

/**
  * Segments events into sessions delimited by explicit start and end session events (instead of e.g. time-based sessions).
  * Events are re-keyed with a session key, and immediately forwarded to a session-events topic.
  *
  * It uses an in-memory state store to store session state. This state store is backed-up by a kafka changelog topic.
  */
class Segmenter(config: AppConfig, serdes: SegmenterSerdes) {

  val settings: Map[String, String] = Map(
    APPLICATION_ID_CONFIG                                       -> "segmenter",
    BOOTSTRAP_SERVERS_CONFIG                                    -> config.kafka.bootstrapServers,
    producerPrefix(ProducerConfig.ACKS_CONFIG)                  -> "all",
    PROCESSING_GUARANTEE_CONFIG                                 -> EXACTLY_ONCE,
    COMMIT_INTERVAL_MS_CONFIG                                   -> "500",
    NUM_STANDBY_REPLICAS_CONFIG                                 -> "1",
    producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG)      -> "lz4",
    DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG      -> classOf[LogAndContinueExceptionHandler].getName,
    DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG                    -> classOf[SegmenterTimestampExtractor].getName,
    consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)    -> "6000",
    consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG) -> "2000",
    METRICS_SAMPLE_WINDOW_MS_CONFIG                             -> "1000",
    METRICS_NUM_SAMPLES_CONFIG                                  -> "5"
  )

  val builder = new StreamsBuilder

  def start(): Unit = {
    val streams = new KafkaStreams(builder.build(), settings.asProps)
    streams.start()
    printMetrics(streams)
  }

  import serdes._

  implicit val sessionMaterialized: Materialized[Device, Session, ByteArrayKeyValueStore] =
    Materialized.as(Stores.inMemoryKeyValueStore("sessions"))

  val events: KStream[Device, Envelope] = builder.stream[Device, Envelope]("events")

  private def rndSessionId(): String = Random.alphanumeric.take(8).mkString

  // upsert sessions by aggregating events by device.
  val sessions: KTable[Device, Session] = events.groupByKey.aggregate[Session](null) { (key, evt, current) =>
    def newSession(completed: Boolean = false) = Session(key, rndSessionId(), evt.timestamp, completed)

    evt.event match {
      case DeviceWokeUp() => newSession()

      case AllDataReceived() =>
        if (current != null) current.copy(completed = true, timestamp = evt.timestamp)
        else newSession(completed = true)

      case _ =>
        if (current != null && !current.completed) current.copy(timestamp = evt.timestamp)
        else newSession() // create a new session if no session exists or the previous one was completed
    }
  }

  // eventually purge sessions (completed ones immediately, others via timeout)
  sessions.toStream.process(() => new SessionPurger(), "sessions")

  // replace the event key with a session key based on the current session.
  events
    .join(sessions)((evt, session) => (evt, session.sessionId)) // session lookup.
    .map { case (key, (evt, sessionId)) => SessionKey(key, sessionId) -> evt }
    .to("session-events")

  private def printMetrics(streams: KafkaStreams): Unit = {
    def value(metric: Metric): Double = metric.metricValue.asInstanceOf[Double]

    val metrics     = streams.metrics().asScala
    val processRate = metrics.collectFirst { case (mn, metric) if mn.name == "process-rate" => metric }.get
    val maxLag = metrics.collectFirst {
      case (mn, metric)
          if mn.name == "records-lag-max" && mn.tags.get("client-id").endsWith("StreamThread-1-consumer") =>
        metric
    }.get
    new Thread(() => {
      while (!Thread.currentThread().isInterrupted) {
        println(f"record rate: ${value(processRate)}%1.2f/sec, max-lag: ${value(maxLag)}%1.0f")
        Thread.sleep(5000)
      }
    }).start()
  }
}
