package io.github.olib963.avrocheck.documentation

// tag::include[]
import io.github.olib963.avrocheck._
import io.github.olib963.avrocheck.Implicits._
import org.apache.avro.Schema
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Properties}

import scala.util.Success

object ApplicationProperty extends Properties("My application") {

  val schema = schemaFromResource("user-schema.avsc")

  property("persists users with negative favourite numbers and gives them a bonus") = {
    val generator = for {
      name <- Gen.alphaNumStr
      // Any number in (-inf, -2001] or [-1000, -1]
      favNum <- Gen.oneOf(Gen.negNum[Int].map(_ - 2001), Gen.chooseNum[Int](-1000, -1))
      overrides = overrideKeys("name" -> name, "favourite_number" -> favNum)
      message <- genFromSchema(schema, overrides = overrides)
    } yield (name, message)
    forAll(generator) { case (name, message) =>
      val result = Application.processUser(message)
      result == Success(PersistedWithBonus(name, 10))
    }
  }

  property("gives a double bonus if their favourite number is between -2000 and -1000") = {
    val generator = for {
      name <- Gen.alphaNumStr
      favNum <- Gen.chooseNum[Int](-2000, -1001)
      overrides = overrideKeys("name" -> name, "favourite_number" -> favNum)
      message <- genFromSchema(schema, overrides = overrides)
    } yield (name, message)
    forAll(generator) { case (name, message) =>
      val result = Application.processUser(message)
      result == Success(PersistedWithBonus(name, 20))
    }
  }

  property("persists users with a positive or no favourite number with no bonus") = {
    val generator = for {
      name <- Gen.alphaNumStr
      favNum <- Gen.oneOf(Gen.const(null), Gen.posNum[Int])
      overrides = overrideKeys("name" -> name, "favourite_number" -> favNum)
      message <- genFromSchema(schema, overrides = overrides)
    } yield (name, message)
    forAll(generator) { case (name, message) =>
      val result = Application.processUser(message)
      result == Success(Persisted(name))
    }
  }

  // end::include[]
  private val newSchema: Schema = schemaFromResource("new-user-schema.avsc")

  property("persists new users with negative favourite numbers and gives them a bonus") = {
    val generator = for {
      name <- Gen.alphaNumStr
      // Any number in (-inf, -2001] or [-1000, -1]
      favNum <- Gen.oneOf(Gen.negNum[Int].map(_ - 2001), Gen.chooseNum[Int](-1000, -1))
      overrides = overrideKeys("name" -> name, "favourite_number" -> favNum)
      message <- genFromSchema(newSchema, overrides = overrides)
    } yield (name, message)
    forAll(generator) { case (name, message) =>
      val result = Application.processUser(message)
      result == Success(PersistedWithBonus(name, 10))
    }
  }

  property("gives new users a double bonus if their favourite number is between -2000 and -1000") = {
    val generator = for {
      name <- Gen.alphaNumStr
      favNum <- Gen.chooseNum[Int](-2000, -1001)
      overrides = overrideKeys("name" -> name, "favourite_number" -> favNum)
      message <- genFromSchema(newSchema, overrides = overrides)
    } yield (name, message)
    forAll(generator) { case (name, message) =>
      val result = Application.processUser(message)
      result == Success(PersistedWithBonus(name, 20))
    }
  }

  property("persists new users with a positive or no favourite number with no bonus") = {
    val generator = for {
      name <- Gen.alphaNumStr
      favNum <- Gen.oneOf(Gen.const(null), Gen.posNum[Int])
      overrides = overrideKeys("name" -> name, "favourite_number" -> favNum)
      message <- genFromSchema(newSchema, overrides = overrides)
    } yield (name, message)
    forAll(generator) { case (name, message) =>
      val result = Application.processUser(message)
      result == Success(Persisted(name))
    }
  }

}
