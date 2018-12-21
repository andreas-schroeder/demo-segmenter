package segmenter

import java.time.Instant
import java.util.Properties

import com.sksamuel.avro4s.SchemaFor._
import common.{AppConfig, Demo, KafkaConfig}
import events._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.{BeforeAndAfter, MustMatchers, WordSpec}
import sessions.{Session, SessionKey}
import shapeless.Coproduct

class SegmenterSpec extends WordSpec with MustMatchers with BeforeAndAfter { test =>

  val deviceSerde: Serde[Device]         = new SimpleAvroSerde[Device]
  val envelopeSerde: Serde[Envelope]     = new SimpleAvroSerde[Envelope]
  val sessionKeySerde: Serde[SessionKey] = new SimpleAvroSerde[SessionKey]
  val sessionSerde: Serde[Session]       = new SimpleAvroSerde[Session]

  var testDriver: TopologyTestDriver = _

  val wokeUp: Event          = Coproduct[Event](DeviceWokeUp())
  val dataReceived: Event    = Coproduct[Event](DataReceived("temperature", 5.0))
  val allDataReceived: Event = Coproduct[Event](AllDataReceived())

  "segmenter" should {
    "forward device events" in {
      sendEvent(Device("device-1"), wokeUp)

      val eventRecord = readEvent

      eventRecord.key.device.id mustBe "device-1"
      eventRecord.key.sessionId mustNot be(empty)
      eventRecord.value.event mustBe wokeUp
    }

    "maintain session key for a session" in {
      val device = Device("device-1")
      sendEvent(device, wokeUp)
      sendEvent(device, dataReceived)

      val startRecord = readEvent
      val dataRecord  = readEvent

      startRecord.value.event mustBe wokeUp
      dataRecord.value.event mustBe dataReceived

      startRecord.key mustBe dataRecord.key
    }

    "create new session key for new sessions" in {
      val device = Device("device-1")

      sendEvent(device, wokeUp)
      sendEvent(device, wokeUp)
      sendEvent(device, dataReceived)

      val start1Record = readEvent
      val start2Record = readEvent
      val dataRecord   = readEvent

      start2Record.key mustNot be(start1Record.key)

      dataRecord.key mustBe start2Record.key
    }

    "purge sessions after session is closed" in {
      val store = testDriver.getKeyValueStore[Device, Session]("sessions")

      val device1 = Device("device-2")
      val device2 = Device("device-1")

      sendEvent(device1, wokeUp)
      sendEvent(device1, allDataReceived)

      Option(store.get(device1)) mustBe defined

      sendEvent(device2, wokeUp, Instant.now.plusSeconds(5)) // advance stream time to trigger purger

      Option(store.get(device1)) mustBe None
    }
  }

  val recordFactory = new ConsumerRecordFactory(deviceSerde.serializer, envelopeSerde.serializer)

  def sendEvent(key: Device, event: Event, timestamp: Instant = Instant.now): Unit =
    testDriver.pipeInput(recordFactory.create("events", key, Envelope(event, timestamp), timestamp.toEpochMilli))

  def readEvent: ProducerRecord[SessionKey, Envelope] =
    testDriver.readOutput("session-events", sessionKeySerde.deserializer, envelopeSerde.deserializer)

  before {
    val config = AppConfig(KafkaConfig("not-used:1234", "not-used:1234"), Demo)
    val segmenter = new Segmenter(
      config,
      new SegmenterSerdes {
        implicit val deviceSerde: Serde[Device]         = test.deviceSerde
        implicit val envelopeSerde: Serde[Envelope]     = test.envelopeSerde
        implicit val sessionSerde: Serde[Session]       = test.sessionSerde
        implicit val sessionKeySerde: Serde[SessionKey] = test.sessionKeySerde
      }
    )

    val props = new Properties()
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "session-segmenter")
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "not-used:1234")
    testDriver = new TopologyTestDriver(segmenter.builder.build(), props)
  }

  after {
    testDriver.close()
  }
}
