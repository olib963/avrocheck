package io.github.olib963.avrocheck

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import io.github.olib963.javatest_scala.{AllJavaTestSyntax, Suite}
import org.apache.avro.Conversions.{DecimalConversion, UUIDConversion}
import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic._
import org.apache.avro.io.{Decoder, DecoderFactory, EncoderFactory}

import scala.util.{Success, Try}

object RoundTripTests extends Suite with AvroCheck with AllJavaTestSyntax with PropertyAssertions {

  private val preserialised = Configuration.Default.copy(preserialiseLogicalTypes = true)
  override def tests = Seq(
    suite("Standard round trip",
      test("should generate records with primitives that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-primitives.avsc"))) { record =>
          that(Try(roundTripCustomConversions(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with composite types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-composites.avsc"))) { record =>
          that(Try(roundTripCustomConversions(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with logical types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-logical-types.avsc"))) { record =>
          that(Try(roundTripCustomConversions(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with union types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-unions.avsc"))) { record =>
          that(Try(roundTripCustomConversions(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records of union types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("union-of-records.avsc"))) { record =>
          that(Try(roundTripCustomConversions(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      }),
    suite("Pre serialised round trip",
      test("should generate records with primitives that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-primitives.avsc"), configuration = preserialised)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with composite types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-composites.avsc"), configuration = preserialised)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with logical types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-logical-types.avsc"), configuration = preserialised)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with union types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-unions.avsc"), configuration = preserialised)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records of union types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("union-of-records.avsc"), configuration = preserialised)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      })
  )

  private def roundTripCustomConversions(record: GenericRecord): GenericRecord = {
    val output = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(output, null)
    new GenericDatumWriter[GenericRecord](record.getSchema, Data.genericData).write(record, encoder)
    encoder.flush()

    val bytes = output.toByteArray
    val binaryDecoder = DecoderFactory.get.binaryDecoder(new ByteArrayInputStream(bytes), null)
    NonUTF8Reader(record.getSchema, Data.genericData).read(null, binaryDecoder)
  }

  private def roundTrip(record: GenericRecord): GenericRecord = {
    val output = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(output, null)
    new GenericDatumWriter[GenericRecord](record.getSchema, GenericData.get()).write(record, encoder)
    encoder.flush()

    val bytes = output.toByteArray
    val binaryDecoder = DecoderFactory.get.binaryDecoder(new ByteArrayInputStream(bytes), null)
    NonUTF8Reader(record.getSchema, GenericData.get()).read(null, binaryDecoder)
  }

}

// Reads strings as Strings not UTF8s. If using the default reader the Map[String, String] in composite types won't be considered equal because it is actually Map[Utf8, Utf8].
case class NonUTF8Reader(schema: Schema, data: GenericData) extends GenericDatumReader[GenericRecord](schema, schema, data) {
  override def readString(old: scala.Any, expected: Schema, in: Decoder): AnyRef = in.readString()
}

object Data {
  val genericData: GenericData = {
    val d = new GenericData()
    d.addLogicalTypeConversion(ScalaDecimalConversion)
    d.addLogicalTypeConversion(new UUIDConversion())
    d.addLogicalTypeConversion(new TimeConversions.DateConversion())
    d.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion())
    d.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion())
    d.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion())
    d.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion())
    d
  }

  import org.apache.avro.Conversion
  import org.apache.avro.LogicalType
  import org.apache.avro.Schema

  // Small converter to handle scala bigdecimals
  object ScalaDecimalConversion extends Conversion[BigDecimal] {
    private val conversion = new DecimalConversion()

    override def getConvertedType: Class[BigDecimal] = classOf[BigDecimal]

    override def getLogicalTypeName: String = conversion.getLogicalTypeName

    override def fromFixed(value: GenericFixed, schema: Schema, `type`: LogicalType): BigDecimal =
      conversion.fromFixed(value, schema, `type`)

    override def toFixed(value: BigDecimal, schema: Schema, `type`: LogicalType): GenericFixed =
      conversion.toFixed(value.underlying(), schema, `type`)

    override def fromBytes(value: ByteBuffer, schema: Schema, `type`: LogicalType): BigDecimal =
      conversion.fromBytes(value, schema, `type`)

    override def toBytes(value: BigDecimal, schema: Schema, `type`: LogicalType): ByteBuffer =
      conversion.toBytes(value.underlying(), schema, `type`)
  }

}
