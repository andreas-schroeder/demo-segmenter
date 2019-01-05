package segmenter
import java.time.Duration

import events.Device
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.state.KeyValueStore
import sessions.Session

import scala.concurrent.duration._

/**
  * Cleans up sessions after a configured timeout value. Completed sessions are immediately purged.
  *
  * Note: It is possible to purge immediately
  * 1) due to the single-threaded processing of Kafka Streams topologies - no concurrent events.
  * 2) assuming no out-of-order and late-arriving events need to be considered.
  */
class SessionPurger extends Processor[Device, Session] with Punctuator {

  val timeout: Long = 4.seconds.toMillis

  var sessions: KeyValueStore[Device, Session] = _

  var purging: Cancellable = _

  override def init(context: ProcessorContext): Unit = {
    sessions = context.getStateStore("sessions").asInstanceOf[KeyValueStore[Device, Session]]
    purging = context.schedule(Duration.ofSeconds(4), PunctuationType.STREAM_TIME, this)
  }

  override def process(key: Device, session: Session): Unit =
    if (session != null && session.completed) sessions.delete(key)

  override def punctuate(timestamp: Long): Unit = sessions.all().forEachRemaining { kv =>
    val sessionAge = timestamp - kv.value.timestamp.toEpochMilli
    if (sessionAge >= timeout) sessions.delete(kv.key)
  }

  override def close(): Unit = purging.cancel()
}
