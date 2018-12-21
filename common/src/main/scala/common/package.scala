import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{Locale, Properties}

import pureconfig.generic.EnumCoproductHint

import scala.util.{Failure, Success, Try}

package object common {

  implicit val modeHint: EnumCoproductHint[Mode] = new EnumCoproductHint[Mode]

  final case class AppConfig(kafka: KafkaConfig, mode: Mode)

  sealed trait Mode
  case object Demo      extends Mode
  case object FullSpeed extends Mode

  final case class KafkaConfig(
      bootstrapServers: String,
      schemaRegistry: String
  )

  def withConfig(body: AppConfig => Unit): Unit = {
    import pureconfig.generic.auto._
    Try(pureconfig.loadConfigOrThrow[AppConfig]) match {
      case Success(config) => body(config)
      case Failure(t) =>
        System.err.println(t.getMessage)
        System.exit(1)
    }
  }

  private val fmt = DateTimeFormatter.ofPattern("mm:ss.SSS").withLocale(Locale.GERMANY).withZone(ZoneId.systemDefault())

  implicit class InstantOps(instant: Instant) {
    def shortFormat: String = fmt.format(instant)
  }

  implicit class MapSettingsOps(settings: Map[String, String]) {
    def asProps: Properties = {
      val p = new Properties()
      settings.foreach(kv => p.put(kv._1, kv._2))
      p
    }
  }
}
