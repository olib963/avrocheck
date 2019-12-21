package io.github.olib963.avrocheck

import java.nio.ByteBuffer

import io.github.olib963.javatest_scala.AllJavaTestSyntax
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalacheck.Gen.Parameters
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks

import scala.util.Try

object PrimitiveSchemaTests extends SchemaGeneratorSuite with PropertyChecks with AllJavaTestSyntax {
  override val schemaFile = "record-with-primitives.avsc"

  override def tests =
    Seq(
      test("Schema based generator") {
        // TODO should this just return the primitive object anyway in an NRC?
        forAll(primitiveTypeSchemaGen) { schema =>
          that("Because it should reject all primitive schemas", Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
        }
        pending("I still need to implement a scalacheck function in JT")
      },
      primitiveFieldSuite
    )

  private def primitiveFieldSuite = {
    val expectedForSeed1 = new GenericData.Record(schema)
    expectedForSeed1.put("boolean", true)
    expectedForSeed1.put("double", 3.9481521047910786E247)
    expectedForSeed1.put("float", 2.95789737E12f)
    expectedForSeed1.put("long", -1556412554917007977L)
    expectedForSeed1.put("bytes", ByteBuffer.wrap(Array[Byte](127, 127, 0, 97, 1, 1, 127, 127, -45, -5, -68, -41, -97, 1, -128, 90, 57, 115, -1, 127, -128, 121, -78, 49, 86, -128, 127, 127, 0, 0, 114, 1, 0, 118, 13, 1, -1, 64, 127, -75, -100, 0, 69, 1, 114, 98, -1, -1, -108, -1, 0, -128, -34, 0, -128, -60, 112, 1, -128, 0, 127, -25, 127, 1, -18, -128, -105, 1, 1, 127, -90)))
    expectedForSeed1.put("string", "걤鉖ⲏ컯ᢉ᭽奚䮤檏ᯆᐩ䶏쪂椗⽘腢片髐୽譋⒵랼⽄Ꭽ鑅盔ࢅΨ欚例掎酦쀳㵒囀姎閛홣죿嵓튦守熀⢚蘫ଙ멿㟂侨ႇꌊ耧黅冰")
    expectedForSeed1.put("int", 0)
    expectedForSeed1.put("null", null)

    suite("Generators for records with primitive fields",
      test("generating a random record") {
        val gen = genFromSchema(schema)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedForSeed1)
      },
      test("generating a random record for next seed") {
        val gen = genFromSchema(schema)
        val expectedForSeed2 = new GenericData.Record(schema)
        expectedForSeed2.put("boolean", false)
        expectedForSeed2.put("double", -7.742894263224504E163)
        expectedForSeed2.put("float", -1.7298706E37f)
        expectedForSeed2.put("long", -1L)
        expectedForSeed2.put("bytes", ByteBuffer.wrap(Array[Byte](-29, -92, 57, 1, 0, -1, -128, 0, -1, 1, -99, 14, 1, -20, 75, -1, 127, -128, -27, 126)))
        expectedForSeed2.put("string", "櫯\u200E鼰놊팿ᒲᬩ섨膫ӿ槎糏獺엠㟃環羞跙⩏앛笋䱸畍ꛫ㮏䚓⬇ﳛ崏칾㰌┎径騆籘ꚿⓕ㝨ꟼ噧暜஬ᦘ墢歘⹣༗伒Ո᠅")
        expectedForSeed2.put("int", -1038086538)
        expectedForSeed2.put("null", null)

        recordsShouldMatch(gen(Parameters.default, secondSeed), expectedForSeed2)
      },
      test("generating a random record with implicit overrides") {
        // This will override just the int and string fields
        // PLEASE NOTE: The string and int fields are last in the schema, this is because the random value of each field effects the seed of the next field,
        // this means if we moved, for example, 'long' to be after 'string' and 'int' it's value would change since the previous input would have changed
        val expectedWithOverrides = new GenericData.Record(expectedForSeed1, true)
        expectedWithOverrides.put("string", "mutblrcyxQhLibemuhatxpsgkdjZksfaqdqqXmuihrHvbcyeeZxfqohonvi")
        expectedWithOverrides.put("int", -78)

        implicit val alphaString: Arbitrary[String] = Arbitrary(Gen.alphaStr)
        implicit val negativeInts: Arbitrary[Int] = Arbitrary(Gen.negNum[Int])

        val gen = genFromSchema(schema)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedWithOverrides)
      },
      invalidOverrideSuite,
      validOverrideSuite
    )
  }

  private val invalids = Seq(
    "null" -> "foo",
    "boolean" -> 10,
    "double" -> "string",
    "double" -> 2.0f,
    "double" -> 3L,
    "float" -> true,
    "float" -> 3.0, // is double
    "float" -> 3, // is int
    "long" -> null,
    "long" -> 12, // is int
    "bytes" -> 12,
    "bytes" -> Array[String]("hello"),
    "string" -> 0.0,
    "int" -> "string",
    "int" -> Long.MaxValue)

  private def invalidOverrideSuite = {
    val missingFieldTest = test("you cannot override a key that doesn't exist") {
      implicit val overrides: Overrides = overrideKeys("foo" -> "bar")
      that(Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
    }
    suite("Invalid override types", missingFieldTest +: invalids.map {
      case (key, value) =>
        test(s"Overriding schema field $key with $value") {
          implicit val overrides: Overrides = overrideKeys(key -> value)
          that(Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
        }
    })
  }

  private val validConstants = Seq(
    "boolean" -> false,
    "double" -> 2.0,
    "float" -> 1.0f,
    "long" -> 12L,
    "string" -> "string",
    "int" -> 40,
    "null" -> null)

  private def validOverrideSuite = {
    // Because byte arrays are wrapped we cannot use same method of testing
    val byteArrayTest = test(s"You can override byte arrays") {
      val bytes = Array[Byte](0, 1, 2)
      implicit val overrides: Overrides = overrideKeys("bytes" -> bytes)
      val gen = genFromSchema(schema)
      val generated = gen(Parameters.default, firstSeed).get
      that(generated.get("bytes"), isEqualTo[Object](ByteBuffer.wrap(bytes)))
    }
    suite("Valid constant overrides", byteArrayTest +: validConstants.map {
      case (key, value) =>
        test(s"Should allow value $value for key $key") {
          implicit val overrides: Overrides = overrideKeys(key -> value)
          val gen = genFromSchema(schema)
          val generated = gen(Parameters.default, firstSeed).get
          that(generated.get(key), isEqualTo(value))
        }
    })
  }

}
