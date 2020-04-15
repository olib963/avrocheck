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
import org.scalacheck.Gen
import org.scalacheck.util.Buildable

import scala.io.Source
import scala.math.BigDecimal.RoundingMode
import scala.reflect.ClassTag
import reflect.classTag

trait AvroCheck {

  // Override definitions
  val noOverrides: Overrides = NoOverrides
  def overrideKeys(overrides: (String, Overrides)): Overrides = KeyOverrides(Map(overrides))

  // Second override exists such that the compiler can distinguish between these two functions. Especially useful when using implicit transformations
  def overrideKeys(overrides: (String, Overrides), secondOverride: (String, Overrides), moreOverrides: (String, Overrides)*): Overrides =
    KeyOverrides(Map(overrides +:secondOverride +: moreOverrides: _*))

  def selectNamedUnion(branchName: String, overrides: Overrides = NoOverrides): Overrides = SelectedUnion(branchName, overrides)

  def constantOverride[A](value: A): Overrides = ConstantOverride(value)
  def generatorOverride[A: ClassTag](gen: Gen[A]): Overrides = GeneratorOverrides(gen)

  def arrayOverride(elements: Seq[Overrides]): Overrides = ArrayOverrides(elements)
  def arrayGenerationOverride(sizeGenerator: Gen[Int] = Gen.posNum[Int], elementOverrides: Overrides = NoOverrides): Overrides = ArrayGenerationOverrides(sizeGenerator, elementOverrides)

  // Schema functions
  def schemaFromResource(schemaResource: String): Schema =
    new Schema.Parser().parse(Source.fromResource(schemaResource).mkString)

  // TODO should we allow a more generic implementation such as : Gen[AvroData] where AvroData = Record | Union(record, schema) | Datum(any)?
  def genFromSchema(schema: Schema, configuration: Configuration = Configuration.Default, overrides: Overrides = NoOverrides): Gen[GenericRecord] = schema.getType match {
    case Type.RECORD => recordGenerator(schema, configuration, overrides) match {
      case Right(gen) => gen
      case Left(error) => sys.error(error.errorMessages.mkString("\n"))
    }
    case Type.UNION if CollectionConverters.toScala(schema.getTypes).forall(_.getType == Type.RECORD) =>
      unionGenerator(schema, configuration, overrides) match {
        case Right(gen) => gen.map(_.asInstanceOf[GenericRecord])
        case Left(error) => sys.error(error.errorMessages.mkString("\n"))
      }
    case _ => sys.error(s"Can only create generator for records or a union of records, schema is not supported: $schema")
  }

  // Internal implementation details
  private type AttemptedGen[A] = Either[AvroCheckError, Gen[A]]

  private def generatorFromSchema(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[Any] = schema.getType match {
    case Type.NULL => nullGenerator(overrides)
    case Type.BOOLEAN => booleanGenerator(configuration, overrides)
    case Type.DOUBLE => doubleGenerator(configuration, overrides)
    case Type.FLOAT => floatGenerator(configuration, overrides)
    case Type.INT => intGenerator(schema, configuration, overrides)
    case Type.LONG => longGenerator(schema, configuration, overrides)
    case Type.STRING => stringGenerator(schema, configuration, overrides)
    case Type.BYTES => byteGenerator(schema, configuration, overrides)
    case Type.FIXED => fixedGenerator(schema, configuration, overrides)
    case Type.ENUM => enumGenerator(schema, configuration, overrides)
    case Type.ARRAY => arrayGenerator(schema, configuration, overrides)
    case Type.MAP => mapGenerator(schema, configuration, overrides)
    case Type.UNION => unionGenerator(schema, configuration, overrides)
    case Type.RECORD => recordGenerator(schema, configuration, overrides)
  }

  private def nullGenerator(overrides: Overrides): AttemptedGen[Null] = overrides match {
    case NoOverrides | ConstantOverride(null) => Right(Gen.const(null))
    case other => Left(SimpleError(s"You cannot override a null schema with anything that isn't null. Override passed: $other"))
  }

  private def booleanGenerator(configuration: Configuration, overrides: Overrides): AttemptedGen[Boolean] = overrides match {
    case NoOverrides => Right(configuration.booleanGen)
    case ConstantOverride(bool: Boolean) => Right(Gen.const(bool))
    case GeneratorOverrides(gen, tag) if tag == classTag[Boolean] => Right(gen.map(_.asInstanceOf[Boolean]))
    case other => Left(SimpleError(s"Invalid override passed for boolean schema: $other"))
  }

  private def doubleGenerator(configuration: Configuration, overrides: Overrides): AttemptedGen[Double] = overrides match {
    case NoOverrides => Right(configuration.doubleGen)
    case ConstantOverride(double: Double) => Right(Gen.const(double))
    case GeneratorOverrides(gen, tag) if tag == classTag[Double] => Right(gen.map(_.asInstanceOf[Double]))
    case other => Left(SimpleError(s"Invalid override passed for double schema: $other"))
  }

  private def floatGenerator(configuration: Configuration, overrides: Overrides): AttemptedGen[Float] = overrides match {
    case NoOverrides => Right(configuration.floatGen)
    case ConstantOverride(float: Float) => Right(Gen.const(float))
    case GeneratorOverrides(gen, tag) if tag == classTag[Float] => Right(gen.map(_.asInstanceOf[Float]))
    case other => Left(SimpleError(s"Invalid override passed for float schema: $other"))
  }

  private def intGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[Any] =
    schema.getLogicalType match {
      case _: Date =>
        val generator = overrides match {
          case NoOverrides => Right(configuration.localDateGen)
          case ConstantOverride(localDate: LocalDate) => Right(Gen.const(localDate))
          case GeneratorOverrides(gen, tag) if tag == classTag[LocalDate] => Right(gen.map(_.asInstanceOf[LocalDate]))
          case other => Left(SimpleError(s"Invalid override passed for date schema: $other"))
        }
        generator.map(_.map(date => if (configuration.preserialiseLogicalTypes) date.toEpochDay.toInt else date))
      case _: TimeMillis =>
        val generator = overrides match {
          case NoOverrides => Right(configuration.localTimeGen.map(timeMillis))
          case ConstantOverride(localTime: LocalTime) => Right(Gen.const(timeMillis(localTime)))
          case GeneratorOverrides(gen, tag) if tag == classTag[LocalTime] => Right(gen.map(_.asInstanceOf[LocalTime]))
          case other => Left(SimpleError(s"Invalid override passed for time millis schema: $other"))
        }
        generator.map(_.map(time => if (configuration.preserialiseLogicalTypes) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay).toInt else time))
      case _ => overrides match {
        case NoOverrides => Right(configuration.intGen)
        case ConstantOverride(int: Int) => Right(Gen.const(int))
        case GeneratorOverrides(gen, tag) if tag == classTag[Int] => Right(gen)
        case other => Left(SimpleError(s"Invalid override passed for int schema: $other"))
      }
    }

  private def longGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[Any] =
    schema.getLogicalType match {
      case _: TimestampMillis =>
        val generator = overrides match {
          case NoOverrides => Right(configuration.instantGen.map(dropNanos))
          case ConstantOverride(instant: Instant) => Right(Gen.const(dropNanos(instant)))
          case GeneratorOverrides(gen, tag) if tag == classTag[Instant] => Right(gen.map(_.asInstanceOf[Instant]))
          case other => Left(SimpleError(s"Invalid override passed for timestamp schema: $other"))
        }
        generator.map(_.map { time => if (configuration.preserialiseLogicalTypes) time.toEpochMilli else time })
      case _: TimestampMicros =>
        val generator = overrides match {
          case NoOverrides => Right(configuration.instantGen)
          case ConstantOverride(instant: Instant) => Right(Gen.const(instant))
          case GeneratorOverrides(gen, tag) if tag == classTag[Instant] => Right(gen.map(_.asInstanceOf[Instant]))
          case other => Left(SimpleError(s"Invalid override passed for timestamp schema: $other"))
        }
        generator.map(_.map { time => if (configuration.preserialiseLogicalTypes) toMicros(time) else time })
      case _: TimeMicros =>
        val generator = overrides match {
          case NoOverrides => Right(configuration.localTimeGen.map(timeMicros))
          case ConstantOverride(localTime: LocalTime) => Right(Gen.const(timeMicros(localTime)))
          case GeneratorOverrides(gen, tag) if tag == classTag[LocalTime] => Right(gen.map(_.asInstanceOf[LocalTime]))
          case other => Left(SimpleError(s"Invalid override passed for time micros schema: $other"))
        }
        generator.map(_.map(time => if (configuration.preserialiseLogicalTypes) TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay) else time))
      case _ => overrides match {
        case NoOverrides => Right(configuration.longGen)
        case ConstantOverride(long: Long) => Right(Gen.const(long))
        case GeneratorOverrides(gen, tag) if tag == classTag[Long] => Right(gen)
        case other => Left(SimpleError(s"Invalid override passed for long schema: $other"))
      }
    }

  private val UUID = LogicalTypes.uuid()

  private def stringGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[Any] =
    schema.getLogicalType match {
      case UUID =>
        val generator = overrides match {
          case NoOverrides => Right(configuration.uuidGen)
          case ConstantOverride(uuid: UUID) => Right(Gen.const(uuid))
          case GeneratorOverrides(gen, tag) if tag == classTag[UUID] => Right(gen)
          case other => Left(SimpleError(s"Invalid override passed for uuid schema: $other"))
        }
        generator.map(_.map(uuid => if (configuration.preserialiseLogicalTypes) uuid.toString else uuid))
      case _ => overrides match {
        case NoOverrides => Right(configuration.stringGen)
        case ConstantOverride(string: String) => Right(Gen.const(string))
        case GeneratorOverrides(gen, tag) if tag == classTag[String] => Right(gen)
        case other => Left(SimpleError(s"Invalid override passed for string schema: $other"))
      }
    }

  private val conversion = new DecimalConversion()
  private def byteGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[Any] =
    schema.getLogicalType match {
      case decimal: Decimal =>
        val generator = overrides match {
          case NoOverrides => Right(configuration.bigDecimalGen)
          case ConstantOverride(bd: BigDecimal) => Right(Gen.const(bd))
          case GeneratorOverrides(gen, tag) if tag == classTag[BigDecimal] => Right(gen.map(_.asInstanceOf[BigDecimal]))
          case other => Left(SimpleError(s"Invalid override passed for decimal bytes schema: $other"))
        }
        generator.map(_.
          map(scaleAndCapPrecision(decimal, _)).
          map(decimal => if (configuration.preserialiseLogicalTypes) conversion.toBytes(decimal.underlying(), schema, schema.getLogicalType) else decimal))
      case _ =>
        val byteArrayGen = overrides match {
          case NoOverrides => Right(Gen.containerOf[Array, Byte](configuration.byteGen))
          case ConstantOverride(bytes: Array[Byte]) => Right(Gen.const(bytes))
          case GeneratorOverrides(gen, tag) if tag == classTag[Array[Byte]] => Right(gen.map(_.asInstanceOf[Array[Byte]]))
          case other => Left(SimpleError(s"Invalid override passed for bytes schema: $other, can only override with byte arrays"))
        }
        byteArrayGen.map(_.map(ByteBuffer.wrap))
    }

  private def fixedGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[Any] =
    schema.getLogicalType match {
      case decimal: Decimal =>
        val decimalGen = overrides match {
          case NoOverrides => Right(configuration.bigDecimalGen)
          case ConstantOverride(bd: BigDecimal) => Right(Gen.const(bd))
          case GeneratorOverrides(gen, tag) if tag == classTag[BigDecimal] => Right(gen.map(_.asInstanceOf[BigDecimal]))
          case other => Left(SimpleError(s"Invalid override passed for fixed decimal schema: $other"))
        }
        decimalGen.map(gen =>
          gen.map(scaleAndCapPrecision(decimal, _))
            .map(capFixedDecimal(_, schema.getFixedSize, decimal))
            .map(decimal => if (configuration.preserialiseLogicalTypes) conversion.toFixed(decimal.underlying(), schema, schema.getLogicalType) else decimal))
      case _ =>
        val byteArrayGen = overrides match {
          case NoOverrides => Right(Gen.containerOfN[Array, Byte](schema.getFixedSize, configuration.byteGen))
          case ConstantOverride(bytes: Array[Byte]) =>
            if (bytes.length != schema.getFixedSize)
              Left(SimpleError(s"Must pass a byte array with correct size for $schema Size of passed array was ${bytes.length}"))
            else
              Right(Gen.const(bytes))
          case other => Left(SimpleError(s"Invalid override passed for fixed schema: $other"))
        }
        byteArrayGen.map(_.map(new GenericData.Fixed(schema, _)))
    }

  private def enumGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[EnumSymbol] = {
    val symbolGen = overrides match {
      case NoOverrides => Right(Gen.oneOf(CollectionConverters.toScala(schema.getEnumSymbols)))
      case ConstantOverride(enum: String) =>
        if (schema.getEnumSymbols.contains(enum))
          Right(Gen.const(enum))
        else
          Left(SimpleError(s"Selected enum ($enum) is not in the schema ($schema)"))
      case other => Left(SimpleError(s"Invalid override passed for enum schema. You can select a constant string value or no overrides but $other was passed"))
    }
    symbolGen.map(_.map(new EnumSymbol(schema, _)))
  }

  private def arrayGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[java.util.List[Any]] = overrides match {
    case ArrayOverrides(elements) =>
      eitherSequence(elements.map(o => generatorFromSchema(schema.getElementType, configuration, o)))
        .map(Gen.sequence(_))
        .left.map(ComposedError("Could not create a list generator", _))
    case ArrayGenerationOverrides(sizeGen, elementOverrides) => generatorFromSchema(schema.getElementType, configuration, elementOverrides)
      .map(e => sizeGen.flatMap(size => Gen.listOfN(size, e).map(CollectionConverters.toJava(_: Seq[Any]))))
      .left.map(ComposedError("Could not create a list generator", _))
    case _ => generatorFromSchema(schema.getElementType, configuration, overrides)
      .map(e => Gen.listOf(e).map(CollectionConverters.toJava(_: Seq[Any])))
      .left.map(ComposedError("Could not create a list generator", _))
  }

  private def mapGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[java.util.Map[String, Any]] =
    generatorFromSchema(schema.getValueType, configuration, overrides).map { valueGenerator =>
        val entryGen = for {
          key <- configuration.stringGen
          value <- valueGenerator
        } yield key -> value
        Gen.mapOf(entryGen).map(CollectionConverters.toJava(_: Map[String, Any]))
      }.left.map(ComposedError("Could not create generator for values in the map", _))

  private def unionGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[Any] =
    overrides match {
      case NoOverrides =>
        val triedGens = CollectionConverters.toScala(schema.getTypes).map { schema =>
          generatorFromSchema(schema, configuration, NoOverrides)
            .left.map(ComposedError(s"Could not create generator for union branch: ${schema.getFullName}", _))
        }
        eitherSequence(triedGens).flatMap {
          case g :: Nil => Right(g)
          case g1 :: g2 :: gs => Right(Gen.oneOf(g1, g2, gs: _*))
          case Nil => Left(SimpleError(s"Schema: $schema is an empty union, this should be impossible."))
        }
      case SelectedUnion(branch, branchOverrides) =>
        CollectionConverters.toScala(schema.getTypes).find(_.getName == branch)
          .fold[AttemptedGen[Any]](Left(SimpleError(s"Could not find branch $branch in schema $schema"))) { branchSchema =>
          generatorFromSchema(branchSchema, configuration, branchOverrides)
            .left.map(ComposedError(s"Could not create generator for union branch: ${branchSchema.getFullName}", _))
        }
      case other =>
        val results = CollectionConverters.toScala(schema.getTypes).map { schema =>
          generatorFromSchema(schema, configuration, other)
            .left.map(ComposedError(s"Could not create generator for union branch: ${schema.getFullName}", _))
        }.toList
        val gens = results.collect{ case Right(gen) => gen }
        val errors = results.collect{ case Left(error) => error }
        gens match {
          case g :: Nil => Right(g) // Only one generator matches override
          case Nil => Left(UnionFailure(CollectionConverters.toScala(schema.getTypes), errors))
          case _ => Left(SimpleError(s"Override $other matched more than one union in $schema"))
        }
    }

  private def eitherSequence[A, B](s: Seq[Either[A, B]]): Either[A, Seq[B]] = s.foldRight(Right(Nil): Either[A, List[B]]) {
    (e, acc) => for (xs <- acc; x <- e) yield x :: xs
  }

  private def recordGenerator(schema: Schema, configuration: Configuration, overrides: Overrides): AttemptedGen[GenericRecord] = {
    val fieldOverridesFunction: Either[AvroCheckError, String => Overrides] = overrides match {
      case NoOverrides => Right(_ => NoOverrides)
      case KeyOverrides(mapped) =>
        val schemaFieldNames = CollectionConverters.toScala(schema.getFields).map(_.name)
        val invalidKeys = mapped.keys.filterNot(schemaFieldNames.contains(_))
        if (invalidKeys.isEmpty)
          Right(fieldName => mapped.getOrElse(fieldName, NoOverrides))
        else
          Left(SimpleError(s"Invalid keys passed to record override: $invalidKeys, these are not in the schema: $schema"))
      case other => Left(SimpleError(s"Invalid override passed for a record type! Must be a KeyOverrides or NoOverrides but was $other"))
    }
    fieldOverridesFunction.flatMap { overrideFunction =>
      // Create a generator of (fieldName: String, value: Any)
      val fieldGens = CollectionConverters.toScala(schema.getFields)
        .map(field => generatorFromSchema(field.schema(), configuration, overrideFunction(field.name()))
          .map(_.map(field.name() -> _))
          .left.map(error => FieldError(field.name(), error))
        )
      val errors = fieldGens.collect{ case Left(error) => error }
      val generators = fieldGens.collect{ case Right(gen) => gen }
      if(errors.nonEmpty) Left(RecordFailure(schema, errors)) else Right(Gen.sequence(generators)(RecordBuildable(schema)))
    }
  }

  case class RecordBuildable(schema: Schema) extends Buildable[(String, Any), GenericRecord] {
    override def builder = new GenericRecordBuilder(schema)
  }

  private val TEN = BigDecimal(10)

  private def scaleAndCapPrecision(decimal: Decimal, bigDecimal: BigDecimal): BigDecimal = {
    val maxPower = decimal.getPrecision - decimal.getScale
    val maximumValue = TEN.pow(maxPower) - TEN.pow(-decimal.getScale)
    absoluteCap(bigDecimal, maximumValue).setScale(decimal.getScale, RoundingMode.HALF_UP)
  }

  private def capFixedDecimal(bigDecimal: BigDecimal, size: Int, decimal: Decimal): BigDecimal = {
    // First byte at least is used for sign hence size - 1
    val maxUnscaled = BigInt(256).pow(size - 1) - 1
    val max = BigDecimal(maxUnscaled, decimal.getScale)
    absoluteCap(bigDecimal, max)
  }

  private def absoluteCap(bigDecimal: BigDecimal, cap: BigDecimal): BigDecimal =
    if(cap < bigDecimal.abs) cap * bigDecimal.signum else bigDecimal

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

  // Internal Error Values
  sealed trait AvroCheckError { def errorMessages: Seq[String] }
  case class SimpleError(error: String) extends AvroCheckError {
    override def errorMessages: Seq[String] = Seq(error)
  }
  case class ComposedError(error: String, cause: AvroCheckError, prefixErrors: Boolean = false) extends AvroCheckError {
    override def errorMessages: Seq[String] = error +: cause.errorMessages.map("\t" + _)
  }
  case class FieldError(field: String, cause: AvroCheckError) extends AvroCheckError {
    override def errorMessages: Seq[String] = s"Could not create generator for field: $field" +: cause.errorMessages.map("\t" + _)
  }
  case class RecordFailure(schema: Schema, errors: Seq[AvroCheckError]) extends AvroCheckError {
    override def errorMessages: Seq[String] = s"Could not create generator for record: ${schema.getFullName}" +: errors.flatMap(_.errorMessages).map("\t" + _)
  }
  case class UnionFailure(schemas: Seq[Schema], errors: Seq[AvroCheckError])extends AvroCheckError {
    override def errorMessages: Seq[String] = s"Could not create generator for Union: ${schemas.map(_.getFullName).mkString(", ")}" +: errors.flatMap(_.errorMessages).map("\t" + _)
  }

}
