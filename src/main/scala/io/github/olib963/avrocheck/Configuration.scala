package io.github.olib963.avrocheck

import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID

import org.scalacheck.{Arbitrary, Gen}

case class Configuration(longGen: Gen[Long],
                         intGen: Gen[Int],
                         stringGen: Gen[String],
                         booleanGen: Gen[Boolean],
                         doubleGen: Gen[Double],
                         floatGen: Gen[Float],
                         byteGen: Gen[Byte],
                         uuidGen: Gen[UUID],
                         localDateGen: Gen[LocalDate],
                         localTimeGen: Gen[LocalTime],
                         instantGen: Gen[Instant],
                         bigDecimalGen: Gen[BigDecimal],
                         preserialiseLogicalTypes: Boolean)

object Configuration {
  // Provide Arbitraries for java.time classes
  val localDateArb: Arbitrary[LocalDate] = Arbitrary(for {
    epochDay <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
  } yield LocalDate.ofEpochDay(epochDay))

  val localTimeArb: Arbitrary[LocalTime] = Arbitrary(for {
    nanoOfDay <- Gen.chooseNum(LocalTime.MIN.toNanoOfDay, LocalTime.MAX.toNanoOfDay)
  } yield LocalTime.ofNanoOfDay(nanoOfDay))

  // Arbitrary that allows the microseconds of an instant to be Long.MAX and Long.MIN
  val instantMicrosArb: Arbitrary[Instant] = Arbitrary(for {
    millis <- Gen.chooseNum(Long.MinValue / 1000L, Long.MaxValue / 1000L)
  } yield Instant.ofEpochMilli(millis))

  val Default = Configuration(
    Arbitrary.arbLong.arbitrary,
    Arbitrary.arbInt.arbitrary,
    Arbitrary.arbString.arbitrary,
    Arbitrary.arbBool.arbitrary,
    Arbitrary.arbDouble.arbitrary,
    Arbitrary.arbFloat.arbitrary,
    Arbitrary.arbByte.arbitrary,
    Arbitrary.arbUuid.arbitrary,
    localDateArb.arbitrary,
    localTimeArb.arbitrary,
    instantMicrosArb.arbitrary,
    Arbitrary.arbBigDecimal.arbitrary,
    preserialiseLogicalTypes = false
  )

}
