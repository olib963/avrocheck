package io.github.olib963.avrocheck

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.scalatest.{FunSpec, Matchers}

import scala.collection.JavaConverters._

// Define a few utilities for creating Specs that check the generated output from schemas with fixed specs
trait SchemaGeneratorSpec extends FunSpec with Matchers with AvroCheck {

  val primitiveTypeSchemaGen: Gen[Schema] =
    Gen.oneOf(Type.STRING, Type.BOOLEAN, Type.BYTES, Type.DOUBLE, Type.FLOAT, Type.INT, Type.LONG, Type.NULL).map(Schema.create)

  val compositeSchemaGen: Gen[Schema] = for {
    primitive <- primitiveTypeSchemaGen
    array = Schema.createArray(primitive)
    map = Schema.createMap(primitive)
    schema <- Gen.oneOf(array, map,
      Schema.createEnum("foo", "bar", "baz", Seq("a", "b", "c").asJava),
      Schema.createFixed("foo", "bar", "baz", 10))
  } yield schema

  val schemaFile: String
  lazy val schema = schemaFromResource(schemaFile)

  val firstSeed = Seed(10)
  val secondSeed = Seed(1234567890)

  def recordsShouldMatch(generated: Option[GenericRecord], expected: GenericRecord, expectedSchema: Schema = schema): Unit = {
    it(s"should generate a record with the correct schema for ${expectedSchema.getName}") {
      generated should be('defined)
      generated.get.getSchema shouldBe expectedSchema
    }
    for (field <- expectedSchema.getFields.asScala.map(_.name())) {
      it(s"should match the expected value for field: $field") {
        val actual = generated.get
        actual.get(field) shouldBe expected.get(field)
      }
    }
  }


}
