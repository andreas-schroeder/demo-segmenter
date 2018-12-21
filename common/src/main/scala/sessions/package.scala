import java.time.Instant

import events.Device

package object sessions {

  case class Session(device: Device, sessionId: String, timestamp: Instant, completed: Boolean = false)

  case class SessionKey(device: Device, sessionId: String)

}
