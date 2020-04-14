package io.github.olib963.avrocheck

import java.nio.ByteBuffer

import io.github.olib963.javatest.matchers.{ComparableMatchers, Matcher}
import io.github.olib963.javatest.{Assertion, AssertionResult}
import io.github.olib963.javatest_scala.AllJavaTestSyntax
import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import org.apache.avro.generic.GenericRecord
import org.scalacheck.{Arbitrary, Gen}
import org.apache.avro.generic.{GenericRecordBuilder => RecordBuilder}

import scala.util.Try
import ScalaVersionSpecificOrderings._

object PrimitiveSchemaTests extends SchemaGeneratorSuite with AllJavaTestSyntax with PropertyAssertions {
  override val schemaFile = "record-with-primitives.avsc"

  override def tests =
    Seq(
      test("Schema based generator") {
        forAll(primitiveTypeSchemaGen) { schema =>
          that("Because it should reject all primitive schemas", Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
        }
      },
      suite("Generators for records with primitive fields",
        Seq(test("generating a random record with constants in the configuration") {
          val pi = 3.14159265358979
          val configuration = Configuration.Default.copy(
            booleanGen = Gen.const(true),
            doubleGen = Gen.const(pi),
            floatGen = Gen.const(0.5f),
            longGen = Gen.const(1L),
            stringGen = Gen.const("hello"),
            intGen = Gen.const(8)
          )
          val bytes = Array[Byte](1, 2, 3, 4)
          val expectedRecord = new RecordBuilder(schema)
            .set("boolean", true)
            .set("double", pi)
            .set("float", 0.5f)
            .set("long", 1L)
            .set("bytes", ByteBuffer.wrap(bytes))
            .set("string", "hello")
            .set("int", 8)
            .set("null", null)
            .build()
          // TODO how to check bytes without override?
          forAll(genFromSchema(schema, configuration, overrides = overrideKeys("bytes" -> constantOverride(bytes))))(
            r => recordsShouldMatch(r, expectedRecord))
        }, invalidOverrideSuite) ++ validOverrideSuites
      )
    )

  private val invalidConstants = Seq(
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
    "int" -> Long.MaxValue).map { case (key, value) => (key, constantOverride(value)) }

  private val invalidGenerators = Seq(
    ("null", generatorOverride(Gen.alphaLowerStr)),
    ("boolean", generatorOverride(Gen.posNum[Int])),
    ("double", generatorOverride(Gen.posNum[Int])),
    ("float", generatorOverride(Gen.posNum[Int])),
    ("long", generatorOverride(Gen.posNum[Int])),
    ("bytes", generatorOverride(Gen.const(Array[String]("Hello", "World")))),
    ("string", generatorOverride(Gen.posNum[Int])),
    ("int", generatorOverride(Gen.alphaLowerStr))
  )

  private def invalidOverrideSuite = {
    val missingFieldTest = test("you cannot override a key that doesn't exist") {
      that(Try(genFromSchema(schema, overrides = overrideKeys("foo" -> constantOverride("bar")))), isFailure[Gen[GenericRecord]])
    }
    val constantTests = suite("Invalid constant overrides", invalidConstants.map {
      case (key, value) =>
        test(s"Overriding schema field $key with $value") {
          val overrides = overrideKeys(key -> constantOverride(value))
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        }
    })
    val generatorTests = suite("Invalid generator overrides", invalidGenerators.map {
      case (key, o) =>
        test(s"Cannot override schema field $key with a generator of the wrong type") {
          val overrides = overrideKeys(key -> o)
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        }
    })
    suite("Invalid override types", Seq(missingFieldTest, constantTests, generatorTests))
  }

  private val validConstants = Seq(
    ("boolean", false, constantOverride(false)),
    ("double", 2.0, constantOverride(2.0)),
    ("float", 1.0f, constantOverride(1.0f)),
    ("long", 12L, constantOverride(12L)),
    ("string", "string", constantOverride("string")),
    ("int", 40, constantOverride(40)),
    ("null", null, constantOverride(null)))

  // TODO move to JT?
  private def fail(reason: String): Assertion = () => AssertionResult.failure(reason)

  private def isBetween[A](min: A, max: A)(implicit ordering: Ordering[A]): Matcher[A] =
    matcher(s"be between $min and $max")(a => ordering.gteq(a, min) && ordering.lteq(a,  max))

  private val alphaNumChars = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
  private val isAlphanumeric: Matcher[String] = matcher("be alphanumeric")(_.forall(alphaNumChars.contains))

  private def validOverrideSuites = {
    // Because byte arrays are wrapped we cannot use same method of testing
    val byteArrayTest = test(s"You can override byte arrays with a constant") {
      val bytes = Array[Byte](0, 1, 2)
      val overrides = overrideKeys("bytes" -> constantOverride(bytes))
      forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("bytes"), isEqualTo[Object](ByteBuffer.wrap(bytes))))
    }
    Seq(
      suite("Valid constant overrides", byteArrayTest +: validConstants.map {
        case (key, value, o) =>
          test(s"Should allow value $value for key $key") {
            val overrides = overrideKeys(key -> o)
            forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get(key), isEqualTo(value)))
          }
      }),
      suite("Valid generator overrides",
        test(s"Should allow a generator override for booleans") {
          val overrides = overrideKeys("boolean" -> generatorOverride(Arbitrary.arbBool.arbitrary))
          forAll(genFromSchema(schema, overrides = overrides))(r => that(r.get("boolean"), hasType[java.lang.Boolean]))
        },
        test(s"Should allow a generator override for doubles") {
          val overrides = overrideKeys("double" -> generatorOverride(Gen.chooseNum[Double](10, 20)))
          forAll(genFromSchema(schema, overrides = overrides))(r => r.get("double") match {
            case d: java.lang.Double => that(Double.unbox(d), isBetween(10d, 20d))
            case other => fail(s"Expected $other to be a double")
          })
        },
        test(s"Should allow a generator override for floats") {
          val overrides = overrideKeys("float" -> generatorOverride(Gen.chooseNum[Float](-20, -10)))
          forAll(genFromSchema(schema, overrides = overrides))(r => r.get("float") match {
            case f: java.lang.Float => that(Float.unbox(f), isBetween(-20f, -10f))
            case other => fail(s"Expected $other to be a float")
          })
        },
        test(s"Should allow a generator override for longs") {
          val overrides = overrideKeys("long" -> generatorOverride(Gen.negNum[Long]))
          forAll(genFromSchema(schema, overrides = overrides))(r => r.get("long") match {
            case l: java.lang.Long => that(l, ComparableMatchers.isLessThan(Long.box(0)))
            case other => fail(s"Expected $other to be a long")
          })
        },
        test(s"Should allow a generator override for strings") {
          val overrides = overrideKeys("string" -> generatorOverride(Gen.alphaLowerStr))
          forAll(genFromSchema(schema, overrides = overrides))(r => r.get("string") match {
            case s: String => that(s, isAlphanumeric)
            case other => fail(s"Expected $other to be a string")
          })
        },
        test(s"Should allow a generator override for byte arrays") {
          val overrides = overrideKeys("bytes" -> generatorOverride(Gen.containerOfN[Array, Byte](4, Arbitrary.arbByte.arbitrary)))
          forAll(genFromSchema(schema, overrides = overrides))(r => r.get("bytes") match {
            case b: ByteBuffer => that(b.array().toIterable, hasSize[Byte](4))
            case other => fail(s"Expected $other to be a byte buffer")
          })
        },
        test(s"Should allow a generator override for ints") {
          val overrides = overrideKeys("int" -> generatorOverride(Gen.posNum[Int]))
          forAll(genFromSchema(schema, overrides = overrides))(r => r.get("int") match {
            case i: java.lang.Integer => that(i, ComparableMatchers.isGreaterThan(Int.box(0)))
            case other => fail(s"Expected $other to be an int")
          })
        }
      )
    )
  }

}
