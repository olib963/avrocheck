package io.github.olib963.avrocheck.documentation

import io.github.olib963.avrocheck._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen, Properties}

object RecordGeneration extends Properties("generating random values from schema") {

  private val schema: Schema = schemaFromResource("user-schema.avsc")

  property("My explicit test") = forAll(genFromSchema(schema)){
    genericRecord => genericRecord.isInstanceOf[GenericRecord]
  }

  property("My explicit positive age test") =  forAll(genFromSchema(schema, Configuration.Default.copy(intGen = Gen.posNum[Int]))){
    userRecord => userRecord.get("age").asInstanceOf[Int] >= 0
  }

  // Implicit configuration
  import io.github.olib963.avrocheck.Implicits._
  property("My implicit test") = forAll(genFromSchemaImplicit(schema)){
    genericRecord => genericRecord.isInstanceOf[GenericRecord]
  }

  property("My implicit positive age test") = {
    implicit val onlyPositiveInts: Arbitrary[Int] = Arbitrary(Gen.posNum[Int])
    forAll(genFromSchemaImplicit(schema)){
      userRecord => userRecord.get("age").asInstanceOf[Int] >= 0
    }
  }

}
