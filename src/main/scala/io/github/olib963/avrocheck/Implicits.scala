package io.github.olib963.avrocheck

import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

import io.github.olib963.avrocheck
import io.github.olib963.avrocheck.Overrides.NoOverrides
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalacheck.{Arbitrary, Gen}

import scala.reflect.ClassTag

object Implicits {

  import scala.language.implicitConversions

  case class PreserialiseLogicalTypes(shouldPreserialise: Boolean) extends AnyVal
  /**
   * Allows implicit configuration of whether or not logical types should be pre-serialised.
   * @see [[Implicits.configFromArbitraries]]
   */
  implicit def preserialiseConfig(shouldPreserialise: Boolean): PreserialiseLogicalTypes = PreserialiseLogicalTypes(shouldPreserialise)

  /**
   * Allows implicit configuration of default value generation to be used. For example:
   * {{{
   *   implicit val preserialise: PreserialiseLogicalTypes = true
   *   implicit val alphaNumOnly: Arbitrary[String] = Arbitrary(gen.alphaNumStr)
   *   // All logical types in this generator will be serialised to their primitives and only use alphanumeric strings
   *   val gen: Gen[GenericRecord] = genFromSchemaImplicits(schema)
   * }}}
   */
  implicit def configFromArbitraries(implicit longArb: Arbitrary[Long],
                                     intArb: Arbitrary[Int],
                                     stringArb: Arbitrary[String],
                                     booleanArb: Arbitrary[Boolean],
                                     doubleArb: Arbitrary[Double],
                                     floatArb: Arbitrary[Float],
                                     byteArb: Arbitrary[Byte],
                                     uuidArb: Arbitrary[UUID],
                                     localDateArb: Arbitrary[LocalDate] = Configuration.localDateArb,
                                     localTimeArb: Arbitrary[LocalTime] = Configuration.localTimeArb,
                                     instantArb: Arbitrary[Instant] = Configuration.instantMicrosArb,
                                     bigDecimalArb: Arbitrary[BigDecimal],
                                     preserialiseLogicalTypes: PreserialiseLogicalTypes = PreserialiseLogicalTypes(false)): Configuration =
    Configuration(
      longArb.arbitrary,
      intArb.arbitrary,
      stringArb.arbitrary,
      booleanArb.arbitrary,
      doubleArb.arbitrary,
      floatArb.arbitrary,
      byteArb.arbitrary,
      uuidArb.arbitrary,
      localDateArb.arbitrary,
      localTimeArb.arbitrary,
      instantArb.arbitrary,
      bigDecimalArb.arbitrary,
      preserialiseLogicalTypes = preserialiseLogicalTypes.shouldPreserialise
    )

  /** @see [[AvroCheck.constantOverride]] */
  implicit def overrideFromConstant[A](value: A): Overrides = constantOverride(value)
  /** @see [[AvroCheck.generatorOverride]] */
  implicit def overrideFromGenerator[A: ClassTag](gen: Gen[A]): Overrides = generatorOverride(gen)

  /**
   * Allows implicit definition of generation from schema.
   * @see [[AvroCheck.genFromSchema]]
   */
  def genFromSchemaImplicit(schema: Schema)(implicit configuration: Configuration, overrides: Overrides = NoOverrides): Gen[GenericRecord] =
    avrocheck.genFromSchema(schema, configuration, overrides)

}
