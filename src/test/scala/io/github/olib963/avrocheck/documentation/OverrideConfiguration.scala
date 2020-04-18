package io.github.olib963.avrocheck.documentation

import io.github.olib963.avrocheck.CollectionConverters.toScala
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Properties}
import io.github.olib963.avrocheck._

object OverrideConfiguration extends Properties("Overriding generation") {

  //****************************//
  //  General Record Overrides  //
  //****************************//

  // User schema we have used above in documentation
  private val userSchema = schemaFromResource("user-schema.avsc")

  property("Explicitly override primitive fields") = {
    val overrides = overrideFields( // Override fields in the record by name
      "name" -> constantOverride("oli"), // Always generate the string "oli" for "name"
      "favourite_number" -> generatorOverride(Gen.posNum[Int].map(_ + 1)) // Always generate a positing Int for "favourite_number"
    )
    forAll(genFromSchema(userSchema, overrides = overrides)) { record =>
      val namedOli = record.get("name") == "oli"
      val randomIntAge = record.get("age").isInstanceOf[Int]
      val positiveFavouriteNUmber = record.get("favourite_number").asInstanceOf[Int] > 0
      namedOli && randomIntAge && positiveFavouriteNUmber
    }
  }

  property("Implicitly override primitive fields") = {
    import io.github.olib963.avrocheck.Implicits._
    // Implicitly infer the override type for each field
    implicit val overrides: Overrides = overrideFields(
      "name" -> "oli",
      "favourite_number" -> Gen.posNum[Int].map(_ + 1)
    )
    forAll(genFromSchemaImplicit(userSchema)) { record =>
      val namedOli = record.get("name") == "oli"
      val randomIntAge = record.get("age").isInstanceOf[Int]
      val positiveFavouriteNUmber = record.get("favourite_number").asInstanceOf[Int] > 0
      namedOli && randomIntAge && positiveFavouriteNUmber
    }
  }

  //******************//
  //  Union Overides  //
  //******************//

  // Schema of two records named "Foo" and "Bar". "Foo" has an "int" field of type "int".
  private val unionSchema = schemaFromResource("union-of-records.avsc")
  property("Explicitly select a branch") = {
    val fooOverrides = overrideFields("int" -> constantOverride(10))
    val overrides = selectNamedUnion(
      "Foo", // Selecting the specific "Foo" branch
      overrides = fooOverrides // Within the foo branch we are setting overrides
    )
    forAll(genFromSchema(unionSchema, overrides = overrides)) { record =>
      val correctSchema = record.getSchema.getName == "Foo"
      val always10 = record.get("int") == 10
      correctSchema && always10
    }
  }

  //******************//
  //  Array Overides  //
  //******************//

  // Contains a field called "longArray" with schema {"type": "array", "items": "long"}
  private val compositeSchema = schemaFromResource("record-with-composites.avsc")

  property("Override array generation") = {
    val fiveOrTenPositiveLongs = arrayGenerationOverride(sizeGenerator = Gen.oneOf(5, 10), generatorOverride(Gen.posNum[Long]))
    val overrides = overrideFields("longArray" -> fiveOrTenPositiveLongs)
    forAll(genFromSchema(compositeSchema, overrides = overrides)) { r =>
      val array = toScala(r.get("longArray").asInstanceOf[java.util.List[Long]])
      val elementAssertion = array.forall(_ >= 0) // Array should only contain non negative longs
      val sizeAssertion = array.size == 5 || array.size == 10
      sizeAssertion && elementAssertion
    }
  }

  property("Explicitly override each element in an array") = {
    val positiveLongThenOne = arrayOverride(List(generatorOverride(Gen.posNum[Long]), constantOverride(1L)))
    val overrides = overrideFields("longArray" -> positiveLongThenOne)
    forAll(genFromSchema(compositeSchema, overrides = overrides)) { r =>
      val array = toScala(r.get("longArray").asInstanceOf[java.util.List[Long]])
      val firstElement = array.headOption
      val firstElementAssertion = firstElement.exists(_ >= 0) // First element of the array should only contain non negative longs

      val secondElement = array.tail.headOption
      val secondElementAssertion = secondElement.contains(1L) // Second element of the array should be 1

      val sizeAssertion = array.size == 2
      sizeAssertion && firstElementAssertion && secondElementAssertion
    }
  }

}
