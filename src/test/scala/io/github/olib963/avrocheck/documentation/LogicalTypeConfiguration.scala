package io.github.olib963.avrocheck.documentation

import java.time.LocalDate

import org.apache.avro.Schema
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen, Properties}
import io.github.olib963.avrocheck._

class LogicalTypeConfiguration extends Properties("Logical type configuration"){

  // This schema has a field "date" with schema {"type": "int", "logicalType": "date"}
  val schema: Schema = io.github.olib963.avrocheck.schemaFromResource("record-with-logical-types.avsc")

  private val onlyDaysSinceEpoch = Gen.posNum[Int].map(LocalDate.ofEpochDay(_))
  private val onlyDaysBeforeEpoch = Gen.negNum[Int].map(_ - 1)

  private val overriddenConfig: Configuration = Configuration.Default.copy(
    intGen = onlyDaysBeforeEpoch,
    localDateGen = onlyDaysSinceEpoch
  )

  // Generates a local date not an int
  property("Explicitly override date type") = forAll(genFromSchema(schema, overriddenConfig)) {
    record => record.get("date").isInstanceOf[LocalDate]
  }

  // Serialises the local date to an int for you, but is still using the Gen[LocalDate] not the Gen[Int] to create the value
  property("Explicitly override date type preserialised") = forAll(genFromSchema(schema, overriddenConfig.copy(preserialiseLogicalTypes = true))) {
    record => record.get("date").asInstanceOf[Int] >= 0
  }

  // Using implicit configuration
  import io.github.olib963.avrocheck.Implicits._
  implicit val onlyDaysSinceEpochArb: Arbitrary[LocalDate] = Arbitrary(onlyDaysSinceEpoch)
  implicit val onlyDaysBeforeEpochArb: Arbitrary[Int] = Arbitrary(onlyDaysBeforeEpoch)

  // Generates a local date not an int
  property("Implicitly override date type") = forAll(genFromSchemaImplicit(schema)) {
    record => record.get("date").isInstanceOf[LocalDate]
  }

  // Serialises the local date to an int for you, but is still using the Gen[LocalDate] not the Gen[Int] to create the value
  property("Implicitly override date type preserialised") = {
    implicit val preserialise: PreserialiseLogicalTypes = true
    forAll(genFromSchemaImplicit(schema)) {
      record => record.get("date").asInstanceOf[Int] >= 0
    }
  }

}
