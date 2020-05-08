package io.github.olib963.avrocheck

import io.github.olib963.avrocheck.Overrides._
import io.github.olib963.avrocheck.Generators._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.scalacheck.{Arbitrary, Gen}

import scala.io.Source
import scala.reflect.ClassTag

trait AvroCheck {

  /**
   * @param schema The avro schema to use to generate random values.
   * @param configuration Allows configuration of default generation parameters e.g. the default `Gen` for `String` values.
   * @param overrides Overrides can be used to customise the generation of arbitrarily nested values without affecting the
   *                  generation of other values for example see `AvroCheck.overrideFields`
   * @return A generator that will create a `GenericRecord` from the schema as long as the schema is a RECORD or UNION of RECORDs.
   */
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

  /** Utility function to parse a schema from a resource file. */
  def schemaFromResource(schemaResource: String): Schema =
    new Schema.Parser().parse(Source.fromResource(schemaResource).mkString)

  /** The default override type - doesn't override any value generation and uses the defaults for the schema. */
  val noOverrides: Overrides = NoOverrides

  /**
   * Ensure the same value is returned for every generation e.g. for a schema of type "int" you could pass constantOverride(10).
   * The type of the value passed must mach the type generated for the schema.
   */
  def constantOverride[A](value: A): Overrides = ConstantOverride(value)

  /**
   * Customise the generator used to create a value for the schema. The type of the generator must match the type of the schema,
   * for example for a schema of type "string" you might pass generatorOverride(Gen.alphaNumStr).
   */
  def generatorOverride[A: ClassTag](gen: Gen[A]): Overrides = GeneratorOverrides(gen)

  /**
   * Customise the generation of an array value for a schema of type "array".
   *
   * @param sizeGenerator generates the size of the array. This must be positive (currently not enforced so will cause errors)
   * @param elementOverrides overrides to use for each element in the generated array.
   */
  def arrayGenerationOverride(sizeGenerator: Gen[Int] = Gen.posNum[Int], elementOverrides: Overrides = NoOverrides): Overrides = ArrayGenerationOverrides(sizeGenerator, elementOverrides)

  /**
   * Overrides array generation to create an array that:
   * - Has the exact size of elements
   * - Uses the overrides in elements in order to generate each element of the array.
   */
  def arrayOverride(elements: Seq[Overrides]): Overrides = ArrayOverrides(elements)

  /**
   * Customise the generation of a map value for a schema of type "map"
   *
   * @param sizeGenerator Generates the size of the map. This must be positive (currently not enforced so will cause errors)
   * @param keyGenerator Generator to be used to generate keys for the map.
   * @param valueOverrides overrides to use for each value in the generated map.
   */
  def mapGenerationOverride(sizeGenerator: Gen[Int] = Gen.posNum[Int],
                            keyGenerator: Gen[String] = Arbitrary.arbString.arbitrary,
                            valueOverrides: Overrides = NoOverrides): Overrides = MapGenerationOverrides(sizeGenerator, keyGenerator, valueOverrides)

  def mapOverride(overrides: Map[String, Overrides]): Overrides = NoOverrides

  /**
   * See the overloaded `AvroCheck.overrideFields` function
   */
  def overrideFields(overrides: (String, Overrides)): Overrides = FieldOverrides(Map(overrides))

  /**
   *  For a schema of type "record" you can set an override for any field, any other fields will use default generation.
   *  For example if your schema is:
   *
   *  {{{
   *    {
   *      "namespace": "example.avro",
   *      "type": "record",
   *      "name": "User",
   *      "fields": [
   *        {
   *          "name": "name",
   *          "type": "string"
   *        },
   *        {
   *          "name": "age",
   *          "type": "int"
   *        },
   *        {
   *          "name": "favourite_number",
   *          "type": [
   *            "int",
   *            "null"
   *          ]
   *        },
   *        {
   *          "name": "favourite_colour",
   *          "type": [
   *            "string",
   *            "null"
   *          ],
   *          "default": "null"
   *        }
   *      ]
   *    }
   *  }}}
   *
   *  you would be able to pass.
   *  {{{
   *  overrideFields("name" -> constantOverride("foo"), "age" -> generatorOverride(Gen.posNum[Int]))
   *  }}}
   */
  def overrideFields(overrides: (String, Overrides), secondOverride: (String, Overrides), moreOverrides: (String, Overrides)*): Overrides =
    FieldOverrides(Map(overrides +:secondOverride +: moreOverrides: _*))

  /**
   * Allows to select which schema in a union of schemas to generate values for.
   *
   * @param branchName The name of the schema in the union to select, if the schema is a record then the name (with no namespace) of the record,
   *                   if a primitive then the name of the primitive (e.g. "string")
   * @param overrides The override values to use for the schema branch selected.
   */
  def selectNamedUnion(branchName: String, overrides: Overrides = NoOverrides): Overrides = SelectedUnion(branchName, overrides)

}
