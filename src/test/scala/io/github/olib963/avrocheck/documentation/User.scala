package io.github.olib963.avrocheck.documentation

// tag::include[]
import com.sksamuel.avro4s.{AvroName, AvroSchema, Decoder}
import org.apache.avro.Schema

case class User(name: String, @AvroName("favourite_number") favouriteNumber: Option[Int])

object User {
  val decoder: Decoder[User] = Decoder[User]
  val schema: Schema = AvroSchema[User]
}
// end::include[]