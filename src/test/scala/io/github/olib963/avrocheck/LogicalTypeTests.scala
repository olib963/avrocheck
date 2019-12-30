package io.github.olib963.avrocheck

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

import io.github.olib963.javatest_scala.AllJavaTestSyntax
import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{LogicalTypes, Schema}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Gen.Parameters

import scala.util.Try

object LogicalTypeTests extends SchemaGeneratorSuite with AllJavaTestSyntax with PropertyAssertions {
  override val schemaFile = "record-with-logical-types.avsc"

  private val preserialised = Configuration.Default.copy(preserialiseLogicalTypes = true)

  private val logicalTypeSchemas = List(
    LogicalTypes.date().addToSchema(Schema.create(Type.INT)),
    LogicalTypes.uuid().addToSchema(Schema.create(Type.STRING)),
    LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG)),
    LogicalTypes.timestampMicros().addToSchema(Schema.create(Type.LONG)),
    LogicalTypes.timeMillis().addToSchema(Schema.create(Type.INT)),
    LogicalTypes.timeMicros().addToSchema(Schema.create(Type.LONG)),
    LogicalTypes.decimal(10, 5).addToSchema(Schema.create(Type.BYTES)),
    LogicalTypes.decimal(10, 5).addToSchema(Schema.createFixed("foo", "", "bar.baz", 10))
  )

  override def tests =
    Seq(
      suite("Generator tests", logicalTypeSchemas.map(schema =>
        // TODO should this just return the composite object anyway in an NRC rather than return a failure?
        test(s"Test for ${schema.getLogicalType.getName}")(
          that("Because it should reject all logical type schemas", Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]]))
      )),
      fieldSuite
    )

  private def fieldSuite = {
    val expectedForSeed1 = new GenericData.Record(schema)
    expectedForSeed1.put("uuid", UUID.fromString("aad1f498-5a65-4844-b88c-ac8b94466502"))
    expectedForSeed1.put("timestampMillis", Instant.parse("1970-01-01T00:00:00.001Z"))
    expectedForSeed1.put("timestampMicros", Instant.EPOCH)
    expectedForSeed1.put("timeMicros", LocalTime.MIDNIGHT)
    expectedForSeed1.put("decimalFixed", BigDecimal("0.0000"))
    expectedForSeed1.put("decimal", BigDecimal("8550949123098682117.0000"))
    expectedForSeed1.put("timeMillis", LocalTime.parse("23:59:59.999"))
    expectedForSeed1.put("date", LocalDate.of(5881580, 7, 11))
    suite("Generators for records with logical type fields",
      test("generating a random record") {
        val gen = genFromSchema(schema)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedForSeed1)
      },
      test("generating a random record for next seed") {
        val gen = genFromSchema(schema)
        val expectedForSeed2 = new GenericData.Record(schema)
        expectedForSeed2.put("uuid", UUID.fromString("42273d86-5537-4cc7-8687-dfd589210cd7"))
        expectedForSeed2.put("timestampMillis", Instant.parse("1969-12-31T23:59:59.999Z"))
        expectedForSeed2.put("timestampMicros", Instant.parse("+136641-10-17T05:33:17.319Z"))
        expectedForSeed2.put("timeMicros", LocalTime.parse("23:59:59.999999"))
        expectedForSeed2.put("decimalFixed", BigDecimal("-5.4053"))
        expectedForSeed2.put("decimal", BigDecimal("4.3336"))
        expectedForSeed2.put("timeMillis", LocalTime.MIDNIGHT)
        expectedForSeed2.put("date", LocalDate.ofEpochDay(0))
        recordsShouldMatch(gen(Parameters.default, secondSeed), expectedForSeed2)
      },
      test("generating a random record with implicit overrides") {
        val expectedWithOverrides = new GenericData.Record(expectedForSeed1, false)
        expectedWithOverrides.put("decimal", BigDecimal("-8550949123098682117.0000"))
        // FixedDecimal is 0 so negated it's the same value
        expectedWithOverrides.put("timeMillis", LocalTime.NOON)
        expectedWithOverrides.put("date", LocalDate.of(2999, 12, 31))

        val amTime = for {
          nanoOfDay <- Gen.chooseNum(LocalTime.MIN.toNanoOfDay, LocalTime.NOON.toNanoOfDay)
        } yield LocalTime.ofNanoOfDay(nanoOfDay)

        val thisMillennium = for {
          epochDay <- Gen.chooseNum(LocalDate.of(2000, 1, 1).toEpochDay, LocalDate.of(2999, 12, 31).toEpochDay)
        } yield LocalDate.ofEpochDay(epochDay)

        val negativeDecimal= Arbitrary.arbBigDecimal.arbitrary.map(_.abs.unary_-)

        val newConfig = Configuration.Default.copy(localTimeGen = amTime, localDateGen = thisMillennium, bigDecimalGen = negativeDecimal)

        val gen = genFromSchema(schema, configuration = newConfig)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedWithOverrides)
      },
      invalidSuite,
      validOverrideSuite,
      preserialisedSuite
    )
  }

  private val invalidOverrides = Seq(
    "uuid" -> 10,
    "uuid" -> "e22bccba-17f6-4cd0-9d7b-43a119a60c63", // Even though the string is a UUID the type is wrong
    "timestampMillis" -> false,
    "timestampMillis" -> 0L, // Uses underlying primitive
    "timestampMicros" -> null,
    "timestampMicros" -> 12L, // Uses underlying primitive
    "timeMicros" -> "string",
    "timeMicros" -> 987654321L, // Uses underlying primitive
    "decimal" -> 3.0, // Not a bigdecimal
    "decimal" -> ByteBuffer.wrap(Array(0, 2)), // Uses underlying primitive
    "decimalFixed" -> 12, // Not a bigdecimal
    "decimalFixed" -> new GenericData.Fixed(schema.getField("decimalFixed").schema(), Array(0)), // Uses underlying type
    "timeMillis" -> "23:59:59.999",
    "timeMillis" -> 12, // Uses underlying primitive
    "date" -> "string",
    "date" -> 1234) // Uses underlying primitive

  private def invalidSuite = suite("Invalid overrides", invalidOverrides.map {
    case (key, value) =>
      test(s"Should not allow value $value for key $key") {
        val overrides = overrideKeys(key -> constantOverride(value))
        that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
      }
  })

  private val validConstants = Seq(
    "uuid" -> UUID.fromString("e22bccba-17f6-4cd0-9d7b-43a119a60c63"),
    "timestampMillis" -> Instant.ofEpochMilli(123456789),
    "timestampMicros" -> Instant.ofEpochMilli(987654321),
    "timeMicros" -> LocalTime.NOON,
    "decimal" -> BigDecimal(10),
    "decimalFixed" -> BigDecimal(0.1273),
    "timeMillis" -> LocalTime.ofSecondOfDay(73656),
    "date" -> LocalDate.of(1991, 11, 17))

  private def validOverrideSuite = suite("Valid overrides", validConstants.map {
    case (key, value) => test(s"Should allow value $value for key $key") {
      val overrides = overrideKeys(key -> constantOverride(value))
      val gen = genFromSchema(schema, overrides = overrides)
      val generated = gen(Parameters.default, firstSeed).get
      that(generated.get(key), isEqualTo[AnyRef](value))
    }
  })

  private def preserialisedSuite = suite("Pre serialising",
    test("generating a random record") {
      val expectedPreSerialised = new GenericData.Record(schema)
      expectedPreSerialised.put("uuid", "aad1f498-5a65-4844-b88c-ac8b94466502")
      expectedPreSerialised.put("timestampMillis", 1L)
      expectedPreSerialised.put("timestampMicros", 0L)
      expectedPreSerialised.put("timeMicros", 0L)
      expectedPreSerialised.put("decimalFixed", new GenericData.Fixed(schema.getField("decimalFixed").schema(), Array(0, 0, 0, 0)))
      expectedPreSerialised.put("decimal", ByteBuffer.wrap(Array[Byte](18, 27, 122, -109, 41, -31, -107, 23, -13, 80)))
      expectedPreSerialised.put("timeMillis", 86399999)
      expectedPreSerialised.put("date", 2147483647)
      val gen = genFromSchema(schema, configuration = preserialised)
      recordsShouldMatch(gen(Parameters.default, firstSeed), expectedPreSerialised)
    },
    test("constant overrides") {
      val overrides = overrideKeys(
        "uuid" -> constantOverride(UUID.fromString("e22bccba-17f6-4cd0-9d7b-43a119a60c63")),
        "timestampMillis" -> constantOverride(Instant.ofEpochMilli(123456789)),
        "timestampMicros" -> constantOverride(Instant.ofEpochMilli(987654321)),
        "timeMicros" -> constantOverride(LocalTime.NOON),
        "decimal" -> constantOverride(BigDecimal(10)),
        "decimalFixed" -> constantOverride(BigDecimal(0.1273)),
        "timeMillis" -> constantOverride(LocalTime.ofSecondOfDay(73656)),
        "date" -> constantOverride(LocalDate.of(1991, 11, 17)))

      val expectedPreSerialised = new GenericData.Record(schema)
      expectedPreSerialised.put("uuid", "e22bccba-17f6-4cd0-9d7b-43a119a60c63")
      expectedPreSerialised.put("timestampMillis", 123456789L)
      expectedPreSerialised.put("timestampMicros", 987654321000L)
      expectedPreSerialised.put("timeMicros", 43200000000L)
      expectedPreSerialised.put("decimalFixed", new GenericData.Fixed(schema.getField("decimalFixed").schema(), Array(0, 0, 4, -7)))
      expectedPreSerialised.put("decimal", ByteBuffer.wrap(Array[Byte](1, -122, -96)))
      expectedPreSerialised.put("timeMillis", 73656000)
      expectedPreSerialised.put("date", 7990)
      val gen = genFromSchema(schema, overrides = overrides, configuration = preserialised)
      recordsShouldMatch(gen(Parameters.default, firstSeed), expectedPreSerialised)
    })
}
