import java.time.Instant

import shapeless.{:+:, CNil, Inl, Inr, Poly1}

/**
  * The event model for telemetry data sent by devices.
  */
package object events {

  // Event key for device events.
  case class Device(id: String)

  // Envelope object for wrapping device events, containing additional context info (i.e. timestamp).
  // Note that with schema-registry, we cannot use an Avro union type as the root element type of Kafka record values.
  case class Envelope(event: Event, timestamp: Instant)

  // This represents all device events. With Avro4s, we must encode them as shapeless coproduct.
  type Event = DeviceWokeUp :+: DataReceived :+: AllDataReceived :+: CNil

  case class DeviceWokeUp()

  case class DataReceived(name: String, value: Double) {
    override def toString: String = f"DataReceived($name,$value%1.3f)"
  }

  case class AllDataReceived()

  // Helper extractor methods and Poly1 for simpler handling of coproducts.

  object DeviceWokeUp {
    def unapply(event: Event): Boolean = event match {
      case Inl(_) => true
      case _      => false
    }
  }

  object DataReceived {
    def unapply(event: Event): Option[(String, Double)] = event match {
      case Inr(Inl(DataReceived(name, value))) => Some((name, value))
      case _                                   => None
    }
  }

  object AllDataReceived {
    def unapply(event: Event): Boolean = event match {
      case Inr(Inr(Inl(_))) => true
      case _                => false
    }
  }

  //noinspection TypeAnnotation
  object unlift extends Poly1 {
    implicit def atWakeup = at[DeviceWokeUp](identity)
    implicit def atData   = at[DataReceived](identity)
    implicit def atSent   = at[AllDataReceived](identity)
  }
}
