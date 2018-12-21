package common
import events.{Device, Envelope}
import org.apache.kafka.common.serialization.Serde
import sessions.{Session, SessionKey}

object Serdes {

  def device(schemaRegistry: String): Serde[Device] = AvroSerde[Device](schemaRegistry, key = true)

  def envelope(schemaRegistry: String): Serde[Envelope] = AvroSerde[Envelope](schemaRegistry, key = false)

  def sessionKey(schemaRegistry: String): Serde[SessionKey] = AvroSerde[SessionKey](schemaRegistry, key = true)

  def session(schemaRegistry: String): Serde[Session] = AvroSerde[Session](schemaRegistry, key = false)
}
