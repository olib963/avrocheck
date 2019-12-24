package io.github.olib963.avrocheck.documentation

import com.sksamuel.avro4s.DefaultFieldMapper
import org.apache.avro.generic.GenericRecord

import scala.util.Try

object Application {
  private val BONUS = 10
  def processUser(message: GenericRecord): Try[Result] =
    Try(User.decoder.decode(message, User.schema, DefaultFieldMapper)).map {
      case User(name, Some(favouriteNumber)) if favouriteNumber >= -2000 && favouriteNumber < -1000 =>
        PersistedWithBonus(name, BONUS * 2)
      case User(name, Some(favouriteNumber)) if favouriteNumber < 0 =>
        PersistedWithBonus(name, BONUS)
      case user => Persisted(user.name)
    }

}

sealed trait Result
case class Persisted(name: String) extends Result
case class PersistedWithBonus(name: String, amount: Int) extends Result
