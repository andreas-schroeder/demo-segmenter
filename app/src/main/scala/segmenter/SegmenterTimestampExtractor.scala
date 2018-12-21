package segmenter
import events.Envelope
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import sessions.Session

class SegmenterTimestampExtractor extends TimestampExtractor {

  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = record.value match {
    case Envelope(_, timestamp)      => timestamp.toEpochMilli
    case Session(_, _, timestamp, _) => timestamp.toEpochMilli
    case _ if record.timestamp >= 0  => record.timestamp
    case _                           => System.currentTimeMillis
  }
}
