package io.github.olib963.avrocheck

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

import io.github.olib963.javatest_scala.AllJavaTestSyntax
import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder => RecordBuilder}
import org.apache.avro.{LogicalTypes, Schema}
import org.scalacheck.{Arbitrary, Gen}

import scala.util.Try

object LogicalTypeTests extends SchemaGeneratorSuite with AllJavaTestSyntax with PropertyAssertions {
  override val schemaFile = "record-with-logical-types.avsc"

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
        test(s"Test for ${schema.getLogicalType.getName}")(
          that("Because it should reject all logical type schemas", Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]]))
      )),
      fieldSuite
    )

  private val maxDecimalWith4Bytes = BigDecimal("1677.7215")
  private def fieldSuite = {
    suite("Generators for records with logical type fields",
      test("generating a random record with constants in the configuration") {
        val uuid = UUID.fromString("aad1f498-5a65-4844-b88c-ac8b94466502")
        val instant = Instant.parse("1970-01-01T00:00:00.001Z")
        val time = LocalTime.parse("23:59:59.999")
        val decimal = BigDecimal("8550949123098682117.00")
        val date = LocalDate.of(5881580, 7, 11)

        val configuration = Configuration.Default.copy(
          uuidGen = Gen.const(uuid),
          localTimeGen = Gen.const(time),
          localDateGen = Gen.const(date),
          instantGen = Gen.const(instant),
          bigDecimalGen = Gen.const(decimal)
        )

        val bigDecimalCap = BigDecimal("999.9999") // Max value allowed in the schema
        val expectedRecord = new RecordBuilder(schema)
          .set("uuid", uuid)
          .set("timestampMillis", instant)
          .set("timestampMicros", instant)
          .set("timeMicros", time)
          .set("timeMillis", time)
          .set("decimal", bigDecimalCap)
          .set("decimalFixed", maxDecimalWith4Bytes)
          .set("date", date)
          .build()
        forAll(genFromSchema(schema, configuration))(r => recordsShouldMatch(r, expectedRecord))
      },
      invalidSuite,
      validOverrideSuite,
      preserialisedSuite
    )
  }

  private val invalidConstantOverrides = Seq(
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

  private val invalidGeneratorOverrides = Seq(
    "uuid" -> generatorOverride(Gen.posNum[Float]),
    "uuid" -> generatorOverride(Gen.const("e22bccba-17f6-4cd0-9d7b-43a119a60c63")), // Even though the string is a UUID the type is wrong
    "timestampMillis" -> generatorOverride(Gen.alphaChar),
    "timestampMillis" -> generatorOverride(Gen.posNum[Long]), // Uses underlying primitive
    "timestampMicros" -> generatorOverride(Gen.const(null)),
    "timestampMicros" -> generatorOverride(Gen.posNum[Long]), // Uses underlying primitive
    "timeMicros" -> generatorOverride(Gen.alphaNumStr),
    "timeMicros" -> generatorOverride(Gen.posNum[Long]), // Uses underlying primitive
    "decimal" -> generatorOverride(Gen.posNum[Double]), // Not a bigdecimal
    "decimal" -> generatorOverride(Gen.const(ByteBuffer.wrap(Array(0, 2)))), // Uses underlying primitive
    "decimalFixed" -> generatorOverride(Gen.negNum[Int]), // Not a bigdecimal
    "decimalFixed" -> generatorOverride(Gen.const(new GenericData.Fixed(schema.getField("decimalFixed").schema(), Array(0)))), // Uses underlying type
    "timeMillis" -> generatorOverride(Gen.const("23:59:59.999")),
    "timeMillis" -> generatorOverride(Gen.posNum[Long]), // Uses underlying primitive
    "date" -> generatorOverride(Gen.alphaNumStr),
    "date" -> generatorOverride(Gen.posNum[Long])) // Uses underlying primitive

  private def invalidSuite = suite("Invalid overrides",
    suite("Invalid constants", invalidConstantOverrides.map {
      case (key, value) =>
        test(s"Should not allow value $value for field $key") {
          val overrides = overrideFields(key -> constantOverride(value))
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        }
    }),
    suite("Invalid generators", invalidGeneratorOverrides.map {
      case (key, keyOverride) =>
        test(s"Should not allow an invalid generator for field $key") {
          val overrides = overrideFields(key -> keyOverride)
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        }
    })
  )

  private val validConstants = Seq(
    "uuid" -> UUID.fromString("e22bccba-17f6-4cd0-9d7b-43a119a60c63"),
    "timestampMillis" -> Instant.ofEpochMilli(123456789),
    "timestampMicros" -> Instant.ofEpochMilli(987654321),
    "timeMicros" -> LocalTime.NOON,
    "decimal" -> BigDecimal(10),
    "decimalFixed" -> BigDecimal(0.1273),
    "timeMillis" -> LocalTime.ofSecondOfDay(73656),
    "date" -> LocalDate.of(1991, 11, 17))

  private val instantGenerator = Configuration.instantMicrosArb.arbitrary
  private val localTimeGenerator = Configuration.localTimeArb.arbitrary

  private def validOverrideSuite = suite("Valid overrides",
    suite("Constant Overrides", bigDecimalPrecisionTests +: validConstants.map {
      case (key, value) => test(s"Should allow value $value for field $key") {
        val overrides = overrideFields(key -> constantOverride(value))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get(key), isEqualTo[AnyRef](value)))
      }

    }),
    suite("Generator Overrides", Seq(
      test(s"Should allow a generator override for uuids") {
        val overrides = overrideFields("uuid" -> generatorOverride(Gen.uuid))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("uuid"), hasType[UUID]))
      },
      test(s"Should allow a generator override for timestampMillis") {
        val overrides = overrideFields("timestampMillis" -> generatorOverride(instantGenerator))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("timestampMillis"), hasType[Instant]))
      },
      test(s"Should allow a generator override for timestampMicros") {
        val overrides = overrideFields("timestampMicros" -> generatorOverride(instantGenerator))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("timestampMicros"), hasType[Instant]))
      },
      test(s"Should allow a generator override for timeMicros") {
        val overrides = overrideFields("timeMicros" -> generatorOverride(localTimeGenerator))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("timeMicros"), hasType[LocalTime]))
      },
      test(s"Should allow a generator override for decimal") {
        val overrides = overrideFields("decimal" -> generatorOverride(Arbitrary.arbBigDecimal.arbitrary))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("decimal"), hasType[BigDecimal]))
      },
      test(s"Should allow a generator override for fixed decimals") {
        val biggerThanMaxValue = Arbitrary.arbBigDecimal.arbitrary.suchThat(_ >= 0).map(_ + maxDecimalWith4Bytes)
        val overrides = overrideFields("decimalFixed" -> generatorOverride(biggerThanMaxValue))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("decimalFixed"), isEqualTo[AnyRef](maxDecimalWith4Bytes)))
      },
      test(s"Should allow a generator override for timeMillis") {
        val overrides = overrideFields("timeMillis" -> generatorOverride(localTimeGenerator))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("timeMillis"), hasType[LocalTime]))
      },
      test(s"Should allow a generator override for date") {
        val date = LocalDate.of(2020, 3, 2)
        val overrides = overrideFields("date" -> generatorOverride(Gen.const(date)))
        forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("date"), isEqualTo[AnyRef](date)))
      },
    )))

  private def bigDecimalPrecisionTests = suite("BigDecimal precision capping",
    test("BigDecimals should be positively capped at precision"){
      // Precision 7, scale 4 => must be less that 10^3
      val overrides = overrideFields("decimal" -> constantOverride(BigDecimal(1000)))
      forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("decimal"), isEqualTo[AnyRef](BigDecimal("999.9999"))))
    },
    test("BigDecimals should be negatively capped at precision"){
      // Precision 7, scale 4 => must be less that 10^3
      val overrides = overrideFields("decimal" -> constantOverride(BigDecimal(-1000)))
      forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("decimal"), isEqualTo[AnyRef](BigDecimal("-999.9999"))))
    },
  )

  private def preserialisedSuite = suite("Pre serialising",
    test("generating a random record using constants in the configuration") {
      val uuidString = "aad1f498-5a65-4844-b88c-ac8b94466502"
      val uuid = UUID.fromString(uuidString)
      val instant = Instant.parse("1970-01-01T00:00:00.001Z")
      val time = LocalTime.parse("23:59:59.999")
      val decimal = BigDecimal("8550949123098682117.00")
      val date = LocalDate.of(5881580, 7, 11)

      val configuration = Configuration.Default.copy(
        uuidGen = Gen.const(uuid),
        localTimeGen = Gen.const(time),
        localDateGen = Gen.const(date),
        instantGen = Gen.const(instant),
        bigDecimalGen = Gen.const(decimal),
        preserialiseLogicalTypes = true
      )

      val decimalBytes = Array[Byte](0, -104, -106, 127) // Byte array representation of 999.9999
      val fixedDecimalSchema = schema.getField("decimalFixed").schema()
      val expectedRecord = new RecordBuilder(schema)
        .set("uuid", uuidString)
        .set("timestampMillis", 1L)
        .set("timestampMicros", 1000L)
        .set("timeMicros", 86399999000L)
        .set("timeMillis", 86399999)
        .set("decimal", ByteBuffer.wrap(decimalBytes))
        .set("decimalFixed", new GenericData.Fixed(fixedDecimalSchema, Array[Byte](0, -1, -1, -1))) // Byte array repesentation of 1677.7215
        .set("date", 2147483647)
        .build()
      forAll(genFromSchema(schema, configuration))(r => recordsShouldMatch(r, expectedRecord))
    })
}
