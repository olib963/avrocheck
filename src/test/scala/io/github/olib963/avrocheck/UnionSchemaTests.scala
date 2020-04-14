package io.github.olib963.avrocheck

import io.github.olib963.javatest_scala.AllJavaTestSyntax
import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder => RecordBuilder}
import org.scalacheck.Gen

import scala.util.Try

object UnionSchemaTests extends SchemaGeneratorSuite with AllJavaTestSyntax with PropertyAssertions {
  override val schemaFile: String = "union-of-records.avsc"

  override def tests = {
    val invalidUnionGen = for {
      invalidSubType <- Gen.oneOf(primitiveTypeSchemaGen, compositeSchemaGen, Seq.empty[Gen[Schema]]: _*)
      set <- Gen.containerOf[Set, Schema](invalidSubType) // remove duplicates
    } yield Schema.createUnion(CollectionConverters.toJava(set.toList))
    Seq(
      test(s"Generator tests")(forAll(invalidUnionGen)(schema =>
        that("Because it should reject all logical type schemas", Try(genFromSchema(schema)), isFailure[Gen[GenericRecord]]))),
      unionRecordsSuite
    )
  }

  private def unionRecordsSuite = {
    val fooBranch = schema.getTypes.get(0)
    val barBranch = schema.getTypes.get(1)

    suite("Generating a union of records",
      test("Pick a random branch using constants in configuration") {
        val configuration = Configuration.Default.copy(
          booleanGen = Gen.const(true),
          doubleGen = Gen.const(12d),
          stringGen = Gen.const("hello"),
          intGen = Gen.const(8)
        )
        val expectedFoo = new RecordBuilder(fooBranch)
          .set("boolean", true)
          .set("int", 8)
          .build()

        val expectedBar = new RecordBuilder(barBranch)
          .set("double", 12d)
          .set("string", "hello")
          .set("null", null)
          .build()
        forAll(genFromSchema(schema, configuration))(
          r => recordsShouldMatch(Some(r), expectedFoo, expectedSchema = fooBranch).or(recordsShouldMatch(Some(r), expectedBar, expectedSchema = barBranch)))
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

          val configuration = Configuration.Default.copy(
            doubleGen = Gen.const(12d),
            stringGen = Gen.const("hello"),
          )

          val expectedRecord = new RecordBuilder(barBranch)
            .set("double", 12d)
            .set("string", "hello")
            .set("null", null)
            .build()

          forAll(genFromSchema(schema, configuration, overrides))(r => recordsShouldMatch(Some(r), expectedRecord, expectedSchema = barBranch))
        },
        test("selecting and overriding a branch") {
          val overrides =
            selectNamedUnion("Bar", overrideKeys("double" -> constantOverride(1.0), "string" -> constantOverride("bar")))

          val expectedUnionSelected = new RecordBuilder(barBranch)
            .set("double", 1.0)
            .set("string", "bar")
            .set("null", null)
            .build()

          forAll(genFromSchema(schema, overrides = overrides))(r => recordsShouldMatch(Some(r), expectedUnionSelected, expectedSchema = barBranch))
        }
      )
    )
  }

}
