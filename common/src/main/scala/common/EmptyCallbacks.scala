package common
import java.util

trait EmptyCallbacks {

  def configure(map: util.Map[String, _], b: Boolean): Unit = ()

  def close(): Unit = ()
}
