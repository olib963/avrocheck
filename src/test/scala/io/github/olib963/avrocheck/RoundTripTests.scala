package io.github.olib963.avrocheck

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import io.github.olib963.javatest_scala.{AllJavaTestSyntax, Suite}
import org.apache.avro.Conversions.{DecimalConversion, UUIDConversion}
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.avro.io.{Decoder, DecoderFactory, EncoderFactory}

import scala.util.{Success, Try}

object RoundTripTests extends Suite with AvroCheck with AllJavaTestSyntax with PropertyAssertions {

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
        forAll(genFromSchema(schemaFromResource("record-with-primitives.avsc"), preserialiseLogicalTypes = true)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with composite types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-composites.avsc"), preserialiseLogicalTypes = true)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with logical types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-logical-types.avsc"), preserialiseLogicalTypes = true)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records with union types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("record-with-unions.avsc"), preserialiseLogicalTypes = true)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      },
      test("should generate records of union types that can be serialised") {
        forAll(genFromSchema(schemaFromResource("union-of-records.avsc"), preserialiseLogicalTypes = true)) { record =>
          that(Try(roundTrip(record)), isEqualTo[Try[GenericRecord]](Success(record)))
        }
      }),

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
    d.addLogicalTypeConversion(new UUIDConversion())
    d.addLogicalTypeConversion(ScalaDecimalConversion)
    d.addLogicalTypeConversion(DateConversion)
    d.addLogicalTypeConversion(TimeMicrosConversion)
    d.addLogicalTypeConversion(TimeMillisConversion)
    d.addLogicalTypeConversion(TimestampMicrosConversion)
    d.addLogicalTypeConversion(TimestampMillisConversion)
    d
  }

  import org.apache.avro.Conversion
  import org.apache.avro.LogicalType
  import org.apache.avro.LogicalTypes
  import org.apache.avro.Schema
  import java.time.Instant
  import java.time.LocalDate
  import java.time.LocalTime
  import java.util.concurrent.TimeUnit

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

  // The following conversions are included in avro 1.9 in the class org.apache.avro.data.Jsr310TimeConversions

  object DateConversion extends Conversion[LocalDate] {
    override def getConvertedType: Class[LocalDate] = classOf[LocalDate]

    override def getLogicalTypeName = "date"

    override def fromInt(daysFromEpoch: Integer, schema: Schema, `type`: LogicalType): LocalDate = LocalDate.ofEpochDay(daysFromEpoch.toLong)

    override def toInt(date: LocalDate, schema: Schema, `type`: LogicalType): Integer = {
      val epochDays = date.toEpochDay
      epochDays.toInt
    }

    override def getRecommendedSchema: Schema = LogicalTypes.date.addToSchema(Schema.create(Schema.Type.INT))
  }

  object TimeMillisConversion extends Conversion[LocalTime] {
    override def getConvertedType: Class[LocalTime] = classOf[LocalTime]

    override def getLogicalTypeName = "time-millis"

    override def fromInt(millisFromMidnight: Integer, schema: Schema, `type`: LogicalType): LocalTime = LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(millisFromMidnight.toLong))

    override def toInt(time: LocalTime, schema: Schema, `type`: LogicalType): Integer = TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay).toInt

    override def getRecommendedSchema: Schema = LogicalTypes.timeMillis.addToSchema(Schema.create(Schema.Type.INT))
  }

  object TimeMicrosConversion extends Conversion[LocalTime] {
    override def getConvertedType: Class[LocalTime] = classOf[LocalTime]

    override def getLogicalTypeName = "time-micros"

    override def fromLong(microsFromMidnight: java.lang.Long, schema: Schema, `type`: LogicalType): LocalTime = LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(microsFromMidnight))

    override def toLong(time: LocalTime, schema: Schema, `type`: LogicalType): java.lang.Long = TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay)

    override def getRecommendedSchema: Schema = LogicalTypes.timeMicros.addToSchema(Schema.create(Schema.Type.LONG))
  }

  object TimestampMillisConversion extends Conversion[Instant] {
    override def getConvertedType: Class[Instant] = classOf[Instant]

    override def getLogicalTypeName = "timestamp-millis"

    override def fromLong(millisFromEpoch: java.lang.Long, schema: Schema, `type`: LogicalType): Instant = Instant.ofEpochMilli(millisFromEpoch)

    override def toLong(timestamp: Instant, schema: Schema, `type`: LogicalType): java.lang.Long = timestamp.toEpochMilli

    override def getRecommendedSchema: Schema = LogicalTypes.timestampMillis.addToSchema(Schema.create(Schema.Type.LONG))
  }

  // Micros with milli precision
  object TimestampMicrosConversion extends Conversion[Instant] {
    override def getConvertedType: Class[Instant] = classOf[Instant]

    override def getLogicalTypeName = "timestamp-micros"

    override def fromLong(microsFromEpoch: java.lang.Long, schema: Schema, `type`: LogicalType): Instant =
      Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(microsFromEpoch))


    override def toLong(instant: Instant, schema: Schema, `type`: LogicalType): java.lang.Long =
      TimeUnit.MILLISECONDS.toMicros(instant.toEpochMilli)

    override def getRecommendedSchema: Schema = LogicalTypes.timestampMicros.addToSchema(Schema.create(Schema.Type.LONG))
  }

}
