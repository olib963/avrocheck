package io.github.olib963.avrocheck

import io.github.olib963.javatest_scala.AllJavaTestSyntax
import io.github.olib963.javatest_scala.scalacheck.PropertyAssertions
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder => RecordBuilder}
import org.scalacheck.Gen

import scala.util.Try

object RecordWithUnionTests extends SchemaGeneratorSuite with AllJavaTestSyntax with PropertyAssertions {
  override val schemaFile: String = "record-with-unions.avsc"

  override def tests = {
    Seq(
      test("generating a random record") {
        val configuration = Configuration.Default.copy(
          stringGen = Gen.const("hello"),
          intGen = Gen.const(8),
          longGen = Gen.const(1234L)
        )

        val nullAndString = new RecordBuilder(schema)
          .set("nullableInt", null)
          .set("stringOrLong", "hello")
          .build()

        val nullAndLong = new RecordBuilder(schema)
          .set("nullableInt", null)
          .set("stringOrLong", 1234L)
          .build()

        val intAndString = new RecordBuilder(schema)
          .set("nullableInt", 8)
          .set("stringOrLong", "hello")
          .build()

        val nintAndLong = new RecordBuilder(schema)
          .set("nullableInt", 8)
          .set("stringOrLong", 1234L)
          .build()
        forAll(genFromSchema(schema, configuration)){r =>
          recordsShouldMatch(r, nullAndString)
            .or(recordsShouldMatch(r, nullAndLong))
            .or(recordsShouldMatch(r, intAndString))
            .or(recordsShouldMatch(r, nintAndLong))
        }
      },
      test("should not let you select a type that doesn't exist in the union") {
        val intAsString = {
          val overrides = overrideFields("nullableInt" -> constantOverride("hello"))
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        }
        val stringOrLongAsBoolean = {
          val overrides = overrideFields("stringOrLong" -> constantOverride(false))
          that(Try(genFromSchema(schema, overrides = overrides)), isFailure[Gen[GenericRecord]])
        }
        intAsString.and(stringOrLongAsBoolean)
      },
      test("Valid overrides, selecting a value for one branch of the union"){
        val overrides = overrideFields("nullableInt" -> constantOverride(12), "stringOrLong" -> constantOverride(123L))

        val expectedUnionSelected = new RecordBuilder(schema)
          .set("nullableInt", 12)
          .set("stringOrLong", 123L)
          .build()

        forAll(genFromSchema(schema, overrides = overrides))(r => recordsShouldMatch(r, expectedUnionSelected))
      }
    )
  }

}
