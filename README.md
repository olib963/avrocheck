# Avrocheck

Small library to generate random `GenericRecord`s from a given avro schema.

Simply extend the `AvroCheck` trait and provide it a schema, there is a utility function to read
schemas from a resource file. The schema must either be for a `RECORD` or a `UNION` of `RECORD`s.

```scala
import org.apache.avro.generic.GenericRecord
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Gen
import io.github.olib963.avrocheck.AvroCheck

class MyTest extends PropertyChecks with AvroCheck {
  private val mySchema = schemaFromResource("my-schema.avsc")
  private val gen: Gen[GenericRecord] = genFromSchema(mySchema)
  
  forAll(gen) { record: GenericRecord =>
     // Stuff with record
  }
}
```

To change the default generators used for the underlying values you can provide an implicit `Arbitrary`.

```scala
import io.github.olib963.avrocheck.AvroCheck
import org.scalacheck.{Arbitrary, Gen}

class MyTest extends AvroCheck {
  // Only use positive integers and alpha strings, all other generators remain the same. 
  implicit val positiveInts: Arbitrary[Int] = Arbitrary(Gen.posNum[Int])
  implicit val alphaOnly: Arbitrary[String] = Arbitrary(Gen.alphaStr)

}
```

## Overrides

If you want to customise the generation of your `GenericRecord` even more you can provide an implicit `Overrides` object.

```scala
```

## Logical Types

Logical types will automatically be generated using the types:

* `timestamp-millis` -> `java.time.Instant`
* `timestamp-micros` -> `java.time.Instant`
* `time-millis` -> `java.time.LocalTime`
* `time-micros` -> `java.time.LocalTime`
* `date` -> `java.time.LocalDate`
* `uuid` -> `java.util.UUID`
* `decimal` -> `scala.math.BigDecimal`

If you want to provide overrides or implicit `Arbitrary`s for logical types you must use these types e.g.

```scala
// This will not work 
implicit val onlyDaysSinceEpoch: Arbitrary[Int] = Arbitrary(gen.posNum[Int])


// This will work
implicit val onlyDaysSinceEpoch: Arbitrary[LocalDate] = 
    Arbitrary(gen.posNum[Int].map(LocalDate.ofEpochDay)
```

If you don't want to go through the hassle of adding logical type conversions to your serialiser you can 
set the option `preserialiseLogicalTypes` to `true` and the values will automatically be turned into their underlying primitives.
 You must however still use the correct arbitrary e.g.

```scala
implicit val onlyDaysSinceEpoch: Arbitrary[LocalDate] = 
       Arbitrary(gen.posNum[Int].map(LocalDate.ofEpochDay)
       
val schema = schemaFromResource("my-schema-with-date-type.avsc")
val generator = genFromSchema(schema, preserialiseLogicalTypes = true)

forAll(generator)(record => record.get("dateField") isInstanceOf[Int] )
```


## Confluent Stack Warning

If you are using this library to run integration tests that integrate with Kafka and the confluent stack you should be aware 
of this:

### Schema Registry with Unions

If you are generating messages that are a `UNION` of `RECORD`s at the top level and you are using schema registry
you will want the union schema to be posted for your topic. This means you _cannot_ simply serialise the `GenericRecord`,
instead you will need to do this:

```scala
import org.apache.avro.generic.GenericRecord
import org.scalacheck.Gen
import io.github.olib963.avrocheck.AvroCheck

class MyTest extends AvroCheck {

  // Schema of two records named "Foo" and "Bar" 
  private val unionSchema = schemaFromResource("my-union-schema.avsc")
  private val gen: Gen[GenericRecord] = genFromSchema(unionSchema)
  
  def serialise() {
    val genericRecord = gen.sample.get
    val serialiser = new KafkaAvroSerialiser(new MySchemaRegistryClient())
    
    // This is NOT what you want, this will post the schema for "Foo" or "Bar" only, not the union of both
    serialiser.serialise("my-topic", genericRecord)
    
    // This is what you want, this will post the union schema for the topic and serialise the 
    // genericRecord using "Foo" or "Bar" respectively
    serialiser.serialise("my-topic", new NonRecordContainer(unionSchema, genericRecord))
  }


}
```
