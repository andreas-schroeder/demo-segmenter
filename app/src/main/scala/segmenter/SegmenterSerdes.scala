package segmenter
import common.{AppConfig, Serdes}
import events.{Device, Envelope}
import org.apache.kafka.common.serialization.Serde
import sessions.{Session, SessionKey}

trait SegmenterSerdes {
  implicit val deviceSerde: Serde[Device]
  implicit val envelopeSerde: Serde[Envelope]
  implicit val sessionSerde: Serde[Session]
  implicit val sessionKeySerde: Serde[SessionKey]
}

class SegmenterAvroSerdes(config: AppConfig) extends SegmenterSerdes {

  import config.kafka.schemaRegistry

  implicit val deviceSerde: Serde[Device]         = Serdes.device(schemaRegistry)
  implicit val envelopeSerde: Serde[Envelope]     = Serdes.envelope(schemaRegistry)
  implicit val sessionSerde: Serde[Session]       = Serdes.session(schemaRegistry)
  implicit val sessionKeySerde: Serde[SessionKey] = Serdes.sessionKey(schemaRegistry)
}
