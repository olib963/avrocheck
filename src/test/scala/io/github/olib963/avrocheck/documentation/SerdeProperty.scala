package io.github.olib963.avrocheck.documentation

// tag::include[]
import com.sksamuel.avro4s.DefaultFieldMapper
import org.scalacheck.{Arbitrary, Properties}
import org.scalacheck.Prop.forAll
import io.github.olib963.avrocheck._
import io.github.olib963.avrocheck.Implicits._

import scala.util.{Success, Try}

object SerdeProperty extends Properties("Serde") {

  val schema = schemaFromResource("user-schema.avsc")

  property("deserialises user messages") = forAll(genFromSchema(schema)) { record =>
    Try(User.decoder.decode(record, User.schema, DefaultFieldMapper)).isSuccess
  }

  // Or if you want to be more precise:
  property("deserialises user messages with correct values") = {
    val generator = for {
      name <- Arbitrary.arbString.arbitrary
      favNum <- Arbitrary.arbOption[Int].arbitrary
      favourite_number = favNum.map(Int.box).orNull

      // Notice that here we do not override age (or in the later schema favourite_colour) because these
      // values are of no interest to our application code
      overrides = overrideKeys("name" -> name, "favourite_number" -> favourite_number)
      record <- genFromSchema(schema, overrides = overrides)
    } yield (record, User(name, favNum))
    forAll(generator) { case (record, user) =>
      Try(User.decoder.decode(record, User.schema, DefaultFieldMapper)) == Success(user)
    }
  }

  // end::include[]
  private val newSchema = schemaFromResource("new-user-schema.avsc")

  property("deserialises new user messages") = forAll(genFromSchema(newSchema)) { record =>
    Try(User.decoder.decode(record, User.schema, DefaultFieldMapper)).isSuccess
  }

  property("deserialises new user messages with correct values") = {
    val generator = for {
      name <- Arbitrary.arbString.arbitrary
      favNum <- Arbitrary.arbOption[Int].arbitrary
      favourite_number = favNum.map(Int.box).orNull
      overrides = overrideKeys("name" -> name, "favourite_number" -> favourite_number)
      record <- genFromSchema(newSchema, overrides = overrides)
    } yield (record, User(name, favNum))
    forAll(generator) { case (record, user) =>
      Try(User.decoder.decode(record, User.schema, DefaultFieldMapper)) == Success(user)
    }
  }

}
