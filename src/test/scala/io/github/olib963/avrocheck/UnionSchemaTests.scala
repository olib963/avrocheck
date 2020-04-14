package io.github.olib963.avrocheck

import io.github.olib963.javatest_scala.AllJavaTestSyntax
import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.Gen.Parameters

import scala.util.Try

object UnionSchemaTests extends SchemaGeneratorSuite with AllJavaTestSyntax with PropertyAssertions {
  override val schemaFile: String = "union-of-records.avsc"

  override def tests = {
    val invalidUnionGen = for {
      invalidSubType <- Gen.oneOf(primitiveTypeSchemaGen, compositeSchemaGen, Seq.empty[Gen[Schema]]: _*)
      set <- Gen.containerOf[Set, Schema](invalidSubType) // remove duplicates
    } yield Schema.createUnion(CollectionConverters.toJava(set.toList))
    Seq(
      // TODO should this just return the object anyway in an NRC rather than return a failure?
      test(s"Generator tests")(forAll(invalidUnionGen)(schema =>
        that("Because it should reject all logical type schemas", Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]]))),
      unionRecordsSuite
    )
  }

  private def unionRecordsSuite = {
    val fooBranch = schema.getTypes.get(0)
    val barBranch = schema.getTypes.get(1)

    val expectedForSeed1 = new GenericData.Record(fooBranch)
    expectedForSeed1.put("boolean", true)
    expectedForSeed1.put("int", -931653021)
    suite("Generating a union of records",
      test("generating a random record") {
        val gen = genFromSchema(schema)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedForSeed1, expectedSchema = fooBranch)
      },
      test("generating a random record for next seed") {
        val gen = genFromSchema(schema)
        val expectedForSeed2 = new GenericData.Record(barBranch)
        expectedForSeed2.put("double", -7.742894263224504E163)
        expectedForSeed2.put("string", "㧁ම㳣群尹つ沨攺酻⎻忬契⼳쎒ඞ櫯‎鼰놊팿ᒲ")
        expectedForSeed2.put("null", null)

        recordsShouldMatch(gen(Parameters.default, secondSeed), expectedForSeed2, expectedSchema = barBranch)
      },
      test("generating a random record with changed config") {
        val expectedWithOverrides = new GenericData.Record(expectedForSeed1, true)
        expectedWithOverrides.put("boolean", false)
        expectedWithOverrides.put("int", 23)

        val newConfig = Configuration.Default.copy(booleanGen = Gen.const(false), intGen = Gen.posNum[Int])
        val gen = genFromSchema(schema, configuration = newConfig)
        recordsShouldMatch(gen(Parameters.default, firstSeed), expectedWithOverrides, expectedSchema = fooBranch)
      },
      suite("Invalid overrides",
        test("should not let you select a branch that doesn't exist in the union") {
          val overrides = selectNamedUnion("Baz")
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        },
        test("should not let you set an invalid override for a union") {
          val overrides = constantOverride(2)
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        }
      ),
      suite("Valid overrides",
        test("selecting a specific union branch") {
          val overrides = selectNamedUnion("Bar")

          val expectedUnionSelected = new GenericData.Record(barBranch)
          expectedUnionSelected.put("double", 1.99638128080751E10)
          expectedUnionSelected.put("string", "vzqzfoadmfdki9k1ofcrsjmvFuIJuqen")
          expectedUnionSelected.put("null", null)

          val newConfig = Configuration.Default.copy(stringGen = Gen.alphaNumStr)

          val gen = genFromSchema(schema, configuration = newConfig, overrides = overrides)
          recordsShouldMatch(gen(Parameters.default, firstSeed), expectedUnionSelected, expectedSchema = barBranch)
        },
        test("selecting and overriding a branch") {
          val overrides =
            selectNamedUnion("Bar", overrideKeys("double" -> constantOverride(1.0), "string" -> constantOverride("bar")))

          val expectedUnionSelected = new GenericData.Record(barBranch)
          expectedUnionSelected.put("double", 1.0)
          expectedUnionSelected.put("string", "bar")
          expectedUnionSelected.put("null", null)

          val gen = genFromSchema(schema, overrides = overrides)
          recordsShouldMatch(gen(Parameters.default, firstSeed), expectedUnionSelected, expectedSchema = barBranch)
        }
      )
    )
  }

}
