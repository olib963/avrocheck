package io.github.olib963.avrocheck

import io.github.olib963.avrocheck.Overrides._
import io.github.olib963.avrocheck.Generators._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.scalacheck.Gen

import scala.io.Source
import scala.reflect.ClassTag

trait AvroCheck {

  // Override definitions
  val noOverrides: Overrides = NoOverrides
  def overrideKeys(overrides: (String, Overrides)): Overrides = KeyOverrides(Map(overrides))

  def overrideKeys(overrides: (String, Overrides), secondOverride: (String, Overrides), moreOverrides: (String, Overrides)*): Overrides =
    KeyOverrides(Map(overrides +:secondOverride +: moreOverrides: _*))

  def selectNamedUnion(branchName: String, overrides: Overrides = NoOverrides): Overrides = SelectedUnion(branchName, overrides)

  def constantOverride[A](value: A): Overrides = ConstantOverride(value)
  def generatorOverride[A: ClassTag](gen: Gen[A]): Overrides = GeneratorOverrides(gen)

  def arrayOverride(elements: Seq[Overrides]): Overrides = ArrayOverrides(elements)
  def arrayGenerationOverride(sizeGenerator: Gen[Int] = Gen.posNum[Int], elementOverrides: Overrides = NoOverrides): Overrides = ArrayGenerationOverrides(sizeGenerator, elementOverrides)

  // Schema functions
  def schemaFromResource(schemaResource: String): Schema =
    new Schema.Parser().parse(Source.fromResource(schemaResource).mkString)

  def genFromSchema(schema: Schema, configuration: Configuration = Configuration.Default, overrides: Overrides = NoOverrides): Gen[GenericRecord] = schema.getType match {
    case Type.RECORD => recordGenerator(schema, configuration, overrides) match {
      case Right(gen) => gen
      case Left(error) => sys.error(error.errorMessages.mkString("\n"))
    }
    case Type.UNION if CollectionConverters.toScala(schema.getTypes).forall(_.getType == Type.RECORD) =>
      unionGenerator(schema, configuration, overrides) match {
        case Right(gen) => gen.map(_.asInstanceOf[GenericRecord])
        case Left(error) => sys.error(error.errorMessages.mkString("\n"))
      }
    case _ => sys.error(s"Can only create generator for records or a union of records, schema is not supported: $schema")
  }

}
