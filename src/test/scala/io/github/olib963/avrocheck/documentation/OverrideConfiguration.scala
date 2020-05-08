package io.github.olib963.avrocheck.documentation

import io.github.olib963.avrocheck.CollectionConverters._
import org.scalacheck.Prop.forAll
import org.scalacheck.{Arbitrary, Gen, Properties}
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
      "favourite_number" -> generatorOverride(Gen.posNum[Int].map(_ + 1)) // Always generate a positive Int for "favourite_number"
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
      overrides = fooOverrides // Within the "Foo" branch we are setting overrides
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

  //******************//
  //  Map Overides  //
  //******************//

  // Contains a field called "stringMap" with schema {"type": "map", "values": "string"}
  private val mapSchema = schemaFromResource("record-with-composites.avsc")

  property("Override map generation") = {
    val oneToFourSixLetteredStringsWithNumericKeys = mapGenerationOverride(
      sizeGenerator = Gen.choose(1, 4),
      keyGenerator = Gen.numStr,
      generatorOverride(Gen.listOfN(6, Arbitrary.arbChar.arbitrary).map(_.mkString))
    )
    val overrides = overrideFields("stringMap" -> oneToFourSixLetteredStringsWithNumericKeys)
    forAll(genFromSchema(mapSchema, overrides = overrides)) { r =>
      val map = toScalaMap(r.get("stringMap").asInstanceOf[java.util.Map[String, String]])
      val valueAssertion = map.values.forall(_.length == 6) // All values should have length 6
      val keyAssertion = map.keys.forall(_.toCharArray.forall(Character.isDigit)) // All keys should be numeric strings
      val sizeAssertion = map.size >= 1 && map.size <= 4
      sizeAssertion && keyAssertion && valueAssertion
    }
  }

  property("Explicitly override each entry in the map") = {
    val alphaStringThenBaz = mapOverride(Map(
      "foo" -> generatorOverride(Gen.alphaStr),
      "bar" -> constantOverride("baz")
    ))
    val overrides = overrideFields("stringMap" -> alphaStringThenBaz)
    forAll(genFromSchema(mapSchema, overrides = overrides)) { r =>
      val map = toScalaMap(r.get("stringMap").asInstanceOf[java.util.Map[String, String]])
      // The key foo should map to a string that only contains characters
      val fooAssertion = map.get("foo").exists(_.forall(Character.isLetter))

      // The key bar should map to "baz"
      val barAssertion = map.get("bar").contains("baz")

      val sizeAssertion = map.size == 2
      sizeAssertion && fooAssertion && barAssertion
    }
  }
}
