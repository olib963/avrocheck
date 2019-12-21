package io.github.olib963.avrocheck

import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID
import java.util.concurrent.TimeUnit

import Overrides._
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes._
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalacheck.{Arbitrary, Gen}
import org.scalacheck.util.Buildable

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.math.BigDecimal.RoundingMode
import scala.util.{Failure, Success, Try}

trait AvroCheck {

  def schemaFromResource(schemaResource: String): Schema =
    new Schema.Parser().parse(SourceReader.readSource(schemaResource))

  // TODO better way to express failure that exceptions?
  def genFromSchema(schema: Schema, preserialiseLogicalTypes: Boolean = false)(implicit configuration: Configuration, overrides: Overrides): Gen[GenericRecord] = schema.getType match {
    case Type.RECORD => recordGenerator(schema, configuration, overrides, preserialiseLogicalTypes).get
    case Type.UNION if schema.getTypes.asScala.forall(_.getType == Type.RECORD) =>
      unionGenerator(schema, configuration, overrides, preserialiseLogicalTypes).get.map(_.asInstanceOf[GenericRecord])
    case _ => sys.error(s"Can only create generator for records or a union of records, schema is not supported: $schema")
  }

  private def generatorFromSchema(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[Any]] = schema.getType match {
    case Type.NULL => nullGenerator(overrides)
    case Type.BOOLEAN => booleanGenerator(configuration, overrides)
    case Type.DOUBLE => doubleGenerator(configuration, overrides)
    case Type.FLOAT => floatGenerator(configuration, overrides)
    case Type.INT => intGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
    case Type.LONG => longGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
    case Type.STRING => stringGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
    case Type.BYTES => byteGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
    case Type.FIXED => fixedGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
    case Type.ENUM => enumGenerator(schema, configuration, overrides)
    case Type.ARRAY => arrayGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
    case Type.MAP => mapGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
    case Type.UNION => unionGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
    case Type.RECORD => recordGenerator(schema, configuration, overrides, preserialiseLogicalTypes)
  }

  private def nullGenerator(overrides: Overrides): Try[Gen[Null]] = overrides match {
    case NoOverrides | ConstantOverride(null) => Success(Gen.const(null))
    case other => Failure(new RuntimeException(s"You cannot override a null schema. Override passed: $other"))
  }

  private def booleanGenerator(configuration: Configuration, overrides: Overrides): Try[Gen[Boolean]] = overrides match {
    case NoOverrides => Success(configuration.booleanGen)
    case ConstantOverride(bool: Boolean) => Success(Gen.const(bool))
    case other => Failure(new RuntimeException(s"Invalid override passed for boolean schema: $other"))
  }

  private def doubleGenerator(configuration: Configuration, overrides: Overrides): Try[Gen[Double]] = overrides match {
    case NoOverrides => Success(configuration.doubleGen)
    case ConstantOverride(double: Double) => Success(Gen.const(double))
    case other => Failure(new RuntimeException(s"Invalid override passed for double schema: $other"))
  }

  private def floatGenerator(configuration: Configuration, overrides: Overrides): Try[Gen[Float]] = overrides match {
    case NoOverrides => Success(configuration.floatGen)
    case ConstantOverride(float: Float) => Success(Gen.const(float))
    case other => Failure(new RuntimeException(s"Invalid override passed for float schema: $other"))
  }

  private def intGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[Any]] =
    schema.getLogicalType match {
      case _: Date =>
        val generator = overrides match {
          case NoOverrides => Success(configuration.localDateGen)
          case ConstantOverride(localDate: LocalDate) => Success(Gen.const(localDate))
          case other => Failure(new RuntimeException(s"Invalid override passed for date schema: $other"))
        }
        generator.map(_.map(date => if (preserialiseLogicalTypes) date.toEpochDay.toInt else date))
      case _: TimeMillis =>
        val generator = overrides match {
          case NoOverrides => Success(configuration.localTimeGen.map(timeMillis))
          case ConstantOverride(localTime: LocalTime) => Success(Gen.const(timeMillis(localTime)))
          case other => Failure(new RuntimeException(s"Invalid override passed for time millis schema: $other"))
        }
        generator.map(_.map(time => if (preserialiseLogicalTypes) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay).toInt else time))
      case _ => overrides match {
        case NoOverrides => Success(configuration.intGen)
        case ConstantOverride(int: Int) => Success(Gen.const(int))
        case other => Failure(new RuntimeException(s"Invalid override passed for int schema: $other"))
      }
    }

  private def longGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[Any]] =
    schema.getLogicalType match {
      case _: TimestampMillis =>
        val generator = overrides match {
          case NoOverrides => Success(configuration.instantGen.map(dropNanos))
          case ConstantOverride(instant: Instant) => Success(Gen.const(dropNanos(instant)))
          case other => Failure(new RuntimeException(s"Invalid override passed for timestamp schema: $other"))
        }
        generator.map(_.map { time => if (preserialiseLogicalTypes) time.toEpochMilli else time })
      case _: TimestampMicros =>
        val generator = overrides match {
          case NoOverrides => Success(configuration.instantGen)
          case ConstantOverride(instant: Instant) => Success(Gen.const(instant))
          case other => Failure(new RuntimeException(s"Invalid override passed for timestamp schema: $other"))
        }
        generator.map(_.map { time => if (preserialiseLogicalTypes) toMicros(time) else time })
      case _: TimeMicros =>
        val generator = overrides match {
          case NoOverrides => Success(configuration.localTimeGen.map(timeMicros))
          case ConstantOverride(localTime: LocalTime) => Success(Gen.const(timeMicros(localTime)))
          case other => Failure(new RuntimeException(s"Invalid override passed for time micros schema: $other"))
        }
        generator.map(_.map(time => if (preserialiseLogicalTypes) TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay) else time))
      case _ => overrides match {
        case NoOverrides => Success(configuration.longGen)
        case ConstantOverride(long: Long) => Success(Gen.const(long))
        case other => Failure(new RuntimeException(s"Invalid override passed for long schema: $other"))
      }
    }

  private val UUID = LogicalTypes.uuid()

  private def stringGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[Any]] =
    schema.getLogicalType match {
      case UUID =>
        val generator = overrides match {
          case NoOverrides => Success(configuration.uuidGen)
          case ConstantOverride(uuid: UUID) => Success(Gen.const(uuid))
          case other => Failure(new RuntimeException(s"Invalid override passed for uuid schema: $other"))
        }
        generator.map(_.map(uuid => if (preserialiseLogicalTypes) uuid.toString else uuid))
      case _ => overrides match {
        case NoOverrides => Success(configuration.stringGen)
        case ConstantOverride(string: String) => Success(Gen.const(string))
        case other => Failure(new RuntimeException(s"Invalid override passed for string schema: $other"))
      }
    }

  private val conversion = new DecimalConversion()
  private def byteGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[Any]] =
    schema.getLogicalType match {
      case decimal: Decimal =>
        val generator = overrides match {
          case NoOverrides => Success(configuration.bigDecimalGen.map(scale(decimal, _)))
          case ConstantOverride(bd: BigDecimal) => Success(Gen.const(scale(decimal, bd)))
          case other => Failure(new RuntimeException(s"Invalid override passed for decimal bytes schema: $other"))
        }
        generator.map(_.map(decimal => if (preserialiseLogicalTypes) conversion.toBytes(decimal.underlying(), schema, schema.getLogicalType) else decimal))
      case _ =>
        val byteArrayGen = overrides match {
          case NoOverrides => Success(Gen.containerOf[Array, Byte](configuration.byteGen))
          case ConstantOverride(bytes: Array[Byte]) => Success(Gen.const(bytes))
          case other => Failure(new RuntimeException(s"Invalid override passed for bytes schema: $other, can only override with byte arrays"))
        }
        byteArrayGen.map(_.map(ByteBuffer.wrap))
    }

  private def fixedGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[Any]] =
    schema.getLogicalType match {
      case decimal: Decimal =>
        val decimalGen = overrides match {
          case NoOverrides => Success(configuration.bigDecimalGen)
          case ConstantOverride(bd: BigDecimal) => Success(Gen.const(bd))
          case other => Failure(new RuntimeException(s"Invalid override passed for fixed decimal schema: $other"))
        }
        decimalGen.map(gen =>
          gen.map(scale(decimal, _))
            .map(capFixedDecimal(_, schema.getFixedSize, decimal))
            .map(decimal => if (preserialiseLogicalTypes) conversion.toFixed(decimal.underlying(), schema, schema.getLogicalType) else decimal))
      case _ =>
        val byteArrayGen = overrides match {
          case NoOverrides => Success(Gen.containerOfN[Array, Byte](schema.getFixedSize, configuration.byteGen))
          case ConstantOverride(bytes: Array[Byte]) =>
            if (bytes.length != schema.getFixedSize)
              Failure(new RuntimeException(s"Must pass a byte array with correct size for $schema"))
            else
              Success(Gen.const(bytes))
          case other => Failure(new RuntimeException(s"Invalid override passed for fixed schema: $other"))
        }
        byteArrayGen.map(_.map(new GenericData.Fixed(schema, _)))
    }

  private def enumGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): Try[Gen[EnumSymbol]] = {
    val symbolGen = overrides match {
      case NoOverrides => Success(Gen.oneOf(schema.getEnumSymbols.asScala))
      case ConstantOverride(enum: String) =>
        if (schema.getEnumSymbols.contains(enum))
          Success(Gen.const(enum))
        else
          Failure(new RuntimeException(s"Selected enum ($enum) is not in the schema ($schema)"))
      case other => Failure(new RuntimeException(s"Invalid override passed for enum schema: $other"))
    }
    symbolGen.map(_.map(new EnumSymbol(schema, _)))
  }

  private def arrayGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[java.util.List[Any]]] = {
    val elementGenerator = generatorFromSchema(schema.getElementType, configuration, overrides, preserialiseLogicalTypes)
    elementGenerator.map(e => Gen.listOf(e).map(_.asJava))
  }

  private def mapGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[java.util.Map[String, Any]]] = {
    val valueGenerator = generatorFromSchema(schema.getValueType, configuration, overrides, preserialiseLogicalTypes)
    valueGenerator.transform(
      v => {
        val entryGen = for {
          key <- configuration.stringGen
          value <- v
        } yield key -> value
        Success(Gen.mapOf(entryGen).map(_.asJava))
      },
      error =>
        Failure(SuppressedStackTrace("Could not create generator for values in the map", error))
    )
  }

  private def unionGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[Any]] =
    overrides match {
      case NoOverrides =>
        val triedGens = schema.getTypes.asScala.map { schema =>
          generatorFromSchema(schema, configuration, NoOverrides, preserialiseLogicalTypes)
            .transform(gen => Success(gen), error => Failure(SuppressedStackTrace(s"Could not create generator for union branch: ${schema.getFullName}", error)))
        }.toList
        trySequence(triedGens).flatMap {
          case g :: Nil => Success(g)
          case g1 :: g2 :: gs => Success(Gen.oneOf(g1, g2, gs: _*))
          case Nil => Failure(new RuntimeException(s"Schema: $schema is an empty union, this should be impossible."))
        }
      case SelectedUnion(branch, branchOverrides) =>
        schema.getTypes.asScala.find(_.getName == branch)
          .fold[Try[Gen[Any]]](Failure(new RuntimeException(s"Could not find branch $branch in schema $schema"))) { branchSchema =>
          generatorFromSchema(branchSchema, configuration, branchOverrides, preserialiseLogicalTypes)
            .transform(gen => Success(gen), error => Failure(SuppressedStackTrace(s"Could not create generator for union branch: ${branchSchema.getFullName}", error)))
        }
      case const: ConstantOverride =>
        val (gens, errors) = schema.getTypes.asScala.map { schema =>
          generatorFromSchema(schema, configuration, const, preserialiseLogicalTypes)
            .transform(gen => Success(gen), error => Failure(SuppressedStackTrace(s"Could not create generator for union branch: ${schema.getFullName}", error)))
        }.toList.foldLeft((Seq.empty[Gen[Any]], Seq.empty[Throwable])) {
          case ((success, failure), Success(gen)) => (gen +: success, failure)
          case ((success, failure), Failure(error)) => (success, error +: failure)
        }
        gens match {
          case g :: Nil => Success(g) // Only one generator matches override
          case Nil => Failure(new RuntimeException(s"Could not create overriden union. Errors were:\n${errors.map(_.getMessage).mkString("\n")}"))
          case _ => Failure(new RuntimeException(s"Override $const matched more than one union in $schema"))
        }
      case other => Failure(new RuntimeException(s"Invalid override passed for a union type: $other"))
    }

  private def recordGenerator(schema: Schema, configuration: Configuration, overrides: Overrides, preserialiseLogicalTypes: Boolean): Try[Gen[GenericRecord]] = {
    // TODO should we allow constant GenericRecord overrides?
    val fieldOverridesFunction: Try[String => Overrides] = overrides match {
      case NoOverrides => Success(_ => NoOverrides)
      case KeyOverrides(mapped) =>
        val schemaFieldNames = schema.getFields.asScala.map(_.name)
        val invalidKeys = mapped.keys.filterNot(schemaFieldNames.contains(_))
        if (invalidKeys.isEmpty)
          Success(fieldName => mapped.getOrElse(fieldName, NoOverrides))
        else
          Failure(new RuntimeException(s"Invalid keys passed to record override: $invalidKeys, these are not in the schema: $schema"))
      case other => Failure(new RuntimeException(s"Invalid override passed for a record type! Must be a KeyOverrides or NoOverrides but was $other"))
    }
    fieldOverridesFunction.flatMap { overrideFunction =>
      // Create a generator of (fieldName: String, value: Any)
      val fieldGens = schema.getFields.asScala
        .map(field => generatorFromSchema(field.schema(), configuration, overrideFunction(field.name()), preserialiseLogicalTypes)
          .transform(
            generator => Success(generator.map(field.name() -> _)),
            error => Failure(SuppressedStackTrace(s"Could not create generator for field: ${field.name()}", error)))
        )
      trySequence(fieldGens).map(gens => Gen.sequence(gens)(RecordBuildable(schema)))
        .transform(gen => Success(gen), error => Failure(SuppressedStackTrace(s"Could not create generator for record: ${schema.getFullName}", error)))
    }
  }

  private def trySequence[A](ts: Seq[Try[A]]): Try[Seq[A]] =
    ts.foldRight(Try(Seq.empty[A])) { case (bTry, a) => a.flatMap(as => bTry.map(b => b +: as)) }

  case class RecordBuildable(schema: Schema) extends Buildable[(String, Any), GenericRecord] {
    override def builder = GenericRecordBuilder(schema)
  }

  case class GenericRecordBuilder(schema: Schema) extends mutable.Builder[(String, Any), GenericRecord] {
    var record = new GenericData.Record(schema)

    override def +=(elem: (String, Any)): this.type = {
      record.put(elem._1, elem._2)
      this
    }

    override def clear(): Unit = record = new GenericData.Record(schema)

    override def result(): GenericRecord = record
  }

  private def scale(decimal: Decimal, bigDecimal: BigDecimal): BigDecimal =
    bigDecimal.setScale(decimal.getScale, RoundingMode.HALF_UP)

  private def capFixedDecimal(bigDecimal: BigDecimal, size: Int, decimal: Decimal): BigDecimal = {
    // First byte at least is used for sign hence size - 1
    val maxUnscaled = BigInt(256).pow(size - 1) - 1
    val max = BigDecimal(maxUnscaled, decimal.getScale)
    bigDecimal.abs.min(max) * bigDecimal.signum
  }


  // The following functions drop extra unused precision such that a round trip of serialise -> deserialise gives the same value
  private def timeMillis(localTime: LocalTime): LocalTime = {
    val millis = TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay)
    LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(millis))
  }

  private def timeMicros(localTime: LocalTime): LocalTime = {
    val micros = TimeUnit.NANOSECONDS.toMicros(localTime.toNanoOfDay)
    LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(micros))
  }

  private def dropNanos(instant: Instant) = Instant.ofEpochMilli(instant.toEpochMilli)

  // TODO handle micro precision, current generator only handles millis precision
  private def toMicros(instant: Instant): Long = TimeUnit.MILLISECONDS.toMicros(instant.toEpochMilli)

  // Reach out at the point of calling 'genFromSchema' and find the appropriate arbitrary generators
  import scala.language.implicitConversions

  implicit def configFromArbitraries(implicit longArb: Arbitrary[Long],
                                     intArb: Arbitrary[Int],
                                     stringArb: Arbitrary[String],
                                     booleanArb: Arbitrary[Boolean],
                                     doubleArb: Arbitrary[Double],
                                     floatArb: Arbitrary[Float],
                                     byteArb: Arbitrary[Byte],
                                     uuidArb: Arbitrary[UUID],
                                     localDateArb: Arbitrary[LocalDate] = Configuration.localDateArb,
                                     localTimeArb: Arbitrary[LocalTime] = Configuration.localTimeArb,
                                     instantArb: Arbitrary[Instant] = Configuration.instantMicrosArb,
                                     bigDecimalArb: Arbitrary[BigDecimal]): Configuration =
    Configuration(
      longArb.arbitrary,
      intArb.arbitrary,
      stringArb.arbitrary,
      booleanArb.arbitrary,
      doubleArb.arbitrary,
      floatArb.arbitrary,
      byteArb.arbitrary,
      uuidArb.arbitrary,
      localDateArb.arbitrary,
      localTimeArb.arbitrary,
      instantArb.arbitrary,
      bigDecimalArb.arbitrary)

  def overrideKeys(overrides: (String, Overrides)): Overrides = KeyOverrides(Map(overrides))

  def overrideKeys(overrides: (String, Overrides), secondOverrides: (String, Overrides), moreOverrides: (String, Overrides)*): Overrides =
    KeyOverrides(Map(overrides +: secondOverrides +: moreOverrides: _*))

  def selectNamedUnion(branchName: String, overrides: Overrides = NoOverrides): Overrides = SelectedUnion(branchName, overrides)

  implicit def constantOverride[A](value: A): Overrides = ConstantOverride(value)

}

case class SuppressedStackTrace(message: String, cause: Throwable) extends RuntimeException(message, cause, false, false)
