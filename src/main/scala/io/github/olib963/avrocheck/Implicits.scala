package io.github.olib963.avrocheck

import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

import io.github.olib963.avrocheck.Overrides.{ConstantOverride, NoOverrides}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalacheck.{Arbitrary, Gen}

object Implicits {

  import scala.language.implicitConversions

  case class PreserialiseLogicalTypes(shouldPreserialise: Boolean)
  implicit def preserialiseConfig(shouldPreserialise: Boolean): PreserialiseLogicalTypes = PreserialiseLogicalTypes(shouldPreserialise)

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

  implicit def overrideFromConstant[A](value: A): Overrides = constantOverride(value)
  implicit def overrideFromGenerator[A](gen: Gen[A]): Overrides = generatorOverride(gen)
  // TODO name of this function
  def genFromSchemaImplicits(schema: Schema)(implicit configuration: Configuration, overrides: Overrides = NoOverrides): Gen[GenericRecord] = genFromSchema(schema, configuration, overrides)

}
