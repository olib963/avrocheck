package io.github.olib963.avrocheck

import io.github.olib963.javatest.Assertion
import io.github.olib963.javatest_scala.Suite
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.scalacheck.Gen

// Define a few utilities for creating Specs that check the generated output from schemas with fixed specs
trait SchemaGeneratorSuite extends Suite with AvroCheck {

  val primitiveTypeSchemaGen: Gen[Schema] =
    Gen.oneOf(Type.STRING, Type.BOOLEAN, Type.BYTES, Type.DOUBLE, Type.FLOAT, Type.INT, Type.LONG, Type.NULL).map(Schema.create)

  val compositeSchemaGen: Gen[Schema] = for {
    primitive <- primitiveTypeSchemaGen
    array = Schema.createArray(primitive)
    map = Schema.createMap(primitive)
    schema <- Gen.oneOf(array, map,
      Schema.createEnum("foo", "bar", "baz", CollectionConverters.toJava(Seq("a", "b", "c"))),
      Schema.createFixed("foo", "bar", "baz", 10))
  } yield schema

  val schemaFile: String
  lazy val schema = schemaFromResource(schemaFile)

  import CollectionConverters.toScala
  import io.github.olib963.javatest_scala._

  def recordsShouldMatch(record: GenericRecord, expected: GenericRecord, expectedSchema: Schema = schema): Assertion = {
      val fieldAssertions = toScala(expectedSchema.getFields).map(_.name()).map(field =>
        that(s"The field $field should match", record.get(field), isEqualTo(expected.get(field))))
      all(that("The schema should be the expected one", record.getSchema, isEqualTo(expectedSchema)) +: fieldAssertions.toList)
  }


}
