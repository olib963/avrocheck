package io.github.olib963.avrocheck.documentation

// tag::include[]
import com.sksamuel.avro4s.DefaultFieldMapper
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import io.github.olib963.avrocheck._

import scala.util.Try

object SerdeProperty extends Properties("Serde") {

  val schema = schemaFromResource("user-schema.avsc")

  property("deserialises user messages") = forAll(genFromSchema(schema)) { record =>
    Try(User.decoder.decode(record, User.schema, DefaultFieldMapper)).isSuccess
  }

  // end::include[]
  property("deserialises new user messages") = forAll(genFromSchema(schemaFromResource("new-user-schema.avsc"))) { record =>
    Try(User.decoder.decode(record, User.schema, DefaultFieldMapper)).isSuccess
  }

}
