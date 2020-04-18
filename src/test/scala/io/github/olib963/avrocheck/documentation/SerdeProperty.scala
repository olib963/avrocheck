package io.github.olib963.avrocheck.documentation

// tag::include[]
import com.sksamuel.avro4s.{AvroSchema, Decoder, DefaultFieldMapper, Encoder}
import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Prop.forAll
import io.github.olib963.avrocheck._
import io.github.olib963.avrocheck.Implicits._

import scala.util.{Success, Try}

object SerdeProperty extends Properties("Serde") {

  // This test checks that Avro4s can deserialise a User case class from a generic record created by it's own autogenerated schema
  property("avro4s round trip") = forAll(Gen.resultOf(User.apply(_: String, _: Option[Int]))) {
    user =>
      val encoded = Encoder[User].encode(user, User.schema, DefaultFieldMapper)
      val decoded = User.decoder.decode(encoded, User.schema, DefaultFieldMapper)
      decoded == user
  }

  // The problem with this can be shown by creating a case class with invalid typing that does not match our schema
  case class InvalidUser(name: Boolean, // Name has the wrong type
                         favouriteNumber: Option[Int] // favouriteNumber should be favourite_number
                        )

  // and seeing that the same test would pass
  property("avro4s invalid round trip") = forAll(Gen.resultOf(InvalidUser)) {
    user =>
      val encoded = Encoder[InvalidUser].encode(user, AvroSchema[InvalidUser], DefaultFieldMapper)
      val decoded = Decoder[InvalidUser].decode(encoded, AvroSchema[InvalidUser], DefaultFieldMapper)
      decoded == user
  }

  // Using avrocheck we can instead check that a record created using the full writer schema is compatible with our case class.
  private val schema = schemaFromResource("user-schema.avsc")
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
      overrides = overrideFields("name" -> name, "favourite_number" -> favourite_number)
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
      overrides = overrideFields("name" -> name, "favourite_number" -> favourite_number)
      record <- genFromSchema(newSchema, overrides = overrides)
    } yield (record, User(name, favNum))
    forAll(generator) { case (record, user) =>
      Try(User.decoder.decode(record, User.schema, DefaultFieldMapper)) == Success(user)
    }
  }

}
