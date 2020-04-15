package io.github.olib963.avrocheck

import io.github.olib963.javatest_scala.AllJavaTestSyntax
import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalacheck.Gen
import CollectionConverters._

import scala.util.Try

object CompositeSchemaTests extends SchemaGeneratorSuite with AllJavaTestSyntax with PropertyAssertions {

  override val schemaFile = "record-with-composites.avsc"

  override def tests =
    Seq(
      test("Schema based generator") {
        forAll(compositeSchemaGen) { schema =>
          that("Because it should reject all composite schemas", Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]])
        }
      },
      fieldSuite
    )

  private def isNumeric(string: String) = string.forall(Character.isDigit)

  private def fieldSuite = {
    suite("Generators for records with composite fields",
      test("generating a random record with the appropriate types on each field") {
        val newConfig = Configuration.Default.copy(
          stringGen = Gen.numStr,
          longGen = Gen.negNum[Long],
          byteGen = Gen.const[Byte](0)
        )
        forAll(genFromSchema(schema, configuration = newConfig)){r =>
          val enum = Option(r.get("enum"))
          val expectedEnums = Seq("a", "b", "c")
          val enumAssertion = that(enum.map(_.toString).exists(expectedEnums.contains), s"Enum value ($enum) should be one of $expectedEnums")

          val fixed = Option(r.get("fixed"))
          val fifteen0Bytes = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
          val expectedFixed = new GenericData.Fixed(schema.getField("fixed").schema(), fifteen0Bytes)
          val fixedAssertion = that(fixed.contains(expectedFixed), s"Fixed value ($fixed) should only contain the byte 0")

          val array = Option(r.get("longArray")).map(_.asInstanceOf[java.util.List[Long]]).map(toScala)
          val arrayAssertion = that(array.exists(_.forall(_ <= 0)), s"Array value ($array) should only contain non positive longs")

          val map = Option(r.get("stringMap")).map(_.asInstanceOf[java.util.Map[String, String]]).map(toScalaMap)
          val mapAssertion = that(map.exists(_.forall{ case (key, value) => isNumeric(key) && isNumeric(value)}),
            s"Map value ($map) should only contain strings that are numeric for both keys and values")

          all(Seq(
            that(r.getSchema, isEqualTo(schema)),
            enumAssertion,
            fixedAssertion,
            arrayAssertion,
            mapAssertion))
        }
      },
      suite("Invalid Overrides",
        test("should not allow explicit enum symbol overrides") {
          val enumSymbol = new GenericData.EnumSymbol(schema.getField("enum").schema(), "b")
          val overrides = overrideKeys("enum" -> constantOverride(enumSymbol))
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        },
        test("should not allow you to select an enum that doesn't exist") {
          val overrides = overrideKeys("enum" -> constantOverride("d"))
          that(Try(genFromSchema(schema, overrides = overrides)) , isFailure[Gen[GenericRecord]])
        },
        test("should not allow a fixed override if the byte array is of the wrong size") {
          val smallAssertion = {
            // Too small
            val overrides = overrideKeys("fixed" -> constantOverride(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)))
            that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
          }
          val bigAssertion = {
            // Too big
            val overrides = overrideKeys("fixed" -> constantOverride(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)))
            that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
          }
          smallAssertion.and(bigAssertion)
        }
      ),
      suite("Valid Overrides",
        test("should let you select a specific enum") {
          val enumValue = "c"
          val enumSymbol = new GenericData.EnumSymbol(schema.getField("enum").schema(), enumValue)
          val overrides = overrideKeys("enum" -> constantOverride(enumValue))
          forAll(genFromSchema(schema, overrides = overrides))(r =>
            that(r.get("enum"), isEqualTo[AnyRef](enumSymbol)))
        },
        test("should let you override a fixed byte array with the correct size") {
          val bytes = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
          val fixedBytes = new GenericData.Fixed(schema.getField("fixed").schema(), bytes)
          val overrides = overrideKeys("fixed" -> constantOverride(bytes))
          forAll(genFromSchema(schema, overrides = overrides))(r =>
            that(r.get("fixed"), isEqualTo[AnyRef](fixedBytes)))
        },
        test("should allow you to override array generation") {
          val overrides = overrideKeys("longArray" -> arrayGenerationOverride(sizeGenerator = Gen.oneOf(5, 10), generatorOverride(Gen.posNum[Long])))
          forAll(genFromSchema(schema, overrides = overrides)){r =>
            val array = toScala(r.get("longArray").asInstanceOf[java.util.List[Long]])
            val elementAssertion = that(array.forall(_ >= 0), s"Array value ($array) should only contain non negative longs")
            val sizeAssertion = that(array, hasSize[Long](5)).or(that(array, hasSize[Long](10)))
            sizeAssertion.and(elementAssertion)
          }
        },
        test("should allow you to explicitly set overrides"){
          val overrides = overrideKeys("longArray" -> arrayOverride(List(generatorOverride(Gen.posNum[Long]), constantOverride(1L))))
          forAll(genFromSchema(schema, overrides = overrides)){r =>
            val array = toScala(r.get("longArray").asInstanceOf[java.util.List[Long]])
            val firstElement = array.headOption
            val firstElementAssertion = that(firstElement.exists(_ >= 0), s"First element of the array ($firstElement) should only contain non negative longs")

            val secondElement = array.tail.headOption
            val secondElementAssertion = that(secondElement.contains(1L), s"Second element of the array ($secondElement) should be 1")

            val sizeAssertion = that(array, hasSize[Long](2))
            all(Seq(sizeAssertion, firstElementAssertion, secondElementAssertion))
          }
        }
      )
    )
  }

}
