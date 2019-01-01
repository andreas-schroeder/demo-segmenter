package segmenter

import common._
import events._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig._
import org.apache.kafka.common.Metric
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig._
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
    APPLICATION_ID_CONFIG                                  -> "segmenter",
    BOOTSTRAP_SERVERS_CONFIG                               -> config.kafka.bootstrapServers,
    PROCESSING_GUARANTEE_CONFIG                            -> EXACTLY_ONCE,
    COMMIT_INTERVAL_MS_CONFIG                              -> 500.toString,
    NUM_STANDBY_REPLICAS_CONFIG                            -> "1",
    DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG -> classOf[LogAndContinueExceptionHandler].getName,
    DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG               -> classOf[SegmenterTimestampExtractor].getName,
    METRICS_SAMPLE_WINDOW_MS_CONFIG                        -> "1000",
    METRICS_NUM_SAMPLES_CONFIG                             -> "5",
  )

  import serdes._

  implicit val sessionMaterialized: Materialized[Device, Session, ByteArrayKeyValueStore] =
    Materialized.as(Stores.inMemoryKeyValueStore("sessions"))

  val builder = new StreamsBuilder

  val events: KStream[Device, Envelope] = builder.stream[Device, Envelope]("events")

  private def rndSessionId(): String = Random.alphanumeric.take(8).mkString

  // upsert sessions by aggregating events by device.
  val sessions: KTable[Device, Session] = events.groupByKey.aggregate[Session](null) { (key, value, last) =>
    def newSession(completed: Boolean = false) = Session(key, rndSessionId(), value.timestamp, completed)

    value.event match {
      case DeviceWokeUp() => newSession()

      case AllDataReceived() =>
        if (last != null) last.copy(completed = true, timestamp = value.timestamp)
        else newSession(completed = true)

      case _ =>
        if (last != null && !last.completed) last.copy(timestamp = value.timestamp)
        else newSession() // create a new session if no session exists or the previous one was completed
    }
  }

  // eventually purge sessions (completed ones immediately, others via timeout)
  // the purger does not really process the output from sessions - it uses the sessions stream as
  // clock / time provider to trigger and control purging.
  sessions.toStream.process(() => new SessionPurger(), "sessions")

  // replace the event key with a session key based on the current session.
  events
    .join(sessions)((evt, session) => (evt, session)) // session lookup.
    .map { case (key, (evt, session)) => SessionKey(key, session.sessionId) -> evt }
    .to("session-events")

  def start(): Unit = {
    val streams = new KafkaStreams(builder.build(), settings.asProps)
    streams.start()
    printMetrics(streams)
  }

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
