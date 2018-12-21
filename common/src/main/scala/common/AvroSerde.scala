package common

import java.util

import com.sksamuel.avro4s._
import io.confluent.kafka.serializers._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic._
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization._

class AvroSerde[T: Encoder: Decoder: SchemaFor] extends Serde[T] {

  private implicit val format: RecordFormat[T] = RecordFormat[T]

  // this is the reader's schema to use in Avro schema resolution
  private val readerSchema: Schema = implicitly[SchemaFor[T]].schema

  // deserialization can only handle GenericRecords, so better check schema type at construction time.
  require(readerSchema.getType == Type.RECORD,
          s"Reader schema must be a record schema, but got a ${readerSchema.getType.getName} schema instead")

  private val kafkaSerializer   = new KafkaAvroSerializer()
  private val kafkaDeserializer = new KafkaAvroDeserializer()

  override val serializer: Serializer[T] = new Serializer[T] with EmptyCallbacks {
    override def serialize(topic: String, data: T): Array[Byte] =
      if (data == null) null else kafkaSerializer.serialize(topic, format.to(data))
  }

  override val deserializer: Deserializer[T] = new Deserializer[T] with EmptyCallbacks {
    override def deserialize(topic: String, data: Array[Byte]): T =
      if (data == null) null.asInstanceOf[T]
      else
        kafkaDeserializer.deserialize(topic, data, readerSchema) match {
          case record: GenericRecord => format.from(record)
          case other =>
            throw new SerializationException(
              s"Unable to deserialize: ${other.getClass.getName} is no Avro GenericRecord, value is '$other'")
        }
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    kafkaSerializer.configure(configs, isKey)
    kafkaDeserializer.configure(configs, isKey)
  }

  override def close(): Unit = {
    kafkaSerializer.close()
    kafkaDeserializer.close()
  }
}

object AvroSerde {

  def apply[T: Encoder: Decoder: SchemaFor](schemaRegistry: String, key: Boolean): AvroSerde[T] = {
    val configs = new java.util.HashMap[String, Any]()
    configs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry)
    configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "false")

    val serde = new AvroSerde[T]
    serde.configure(configs, key)
    serde
  }
}
