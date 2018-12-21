package segmenter

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{Decoder, Encoder, RecordFormat, SchemaFor}
import common.EmptyCallbacks
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class SimpleAvroSerde[T: Encoder: Decoder: SchemaFor] extends Serde[T] with EmptyCallbacks {

  private val format = RecordFormat[T]

  private val schemaFor = implicitly[SchemaFor[T]]

  final val serializer: Serializer[T] = new Serializer[T] with EmptyCallbacks {

    val datumWriter = new GenericDatumWriter[GenericRecord](schemaFor.schema)

    override def serialize(topic: String, value: T): Array[Byte] =
      if (value == null) {
        null
      } else {
        val bytes   = new ByteArrayOutputStream()
        val encoder = EncoderFactory.get().directBinaryEncoder(bytes, null)
        datumWriter.write(format.to(value), encoder)
        bytes.toByteArray
      }
  }

  final val deserializer: Deserializer[T] = new Deserializer[T] with EmptyCallbacks {

    val datumReader = new GenericDatumReader[GenericRecord](schemaFor.schema)

    override def deserialize(topic: String, bytes: Array[Byte]): T =
      if (bytes == null) {
        null.asInstanceOf[T]
      } else {
        val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        format.from(datumReader.read(null, decoder))
      }
  }
}
