package io.github.olib963.avrocheck.documentation

import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer, NonRecordContainer}
import io.github.olib963.avrocheck._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalacheck.Prop.forAll
import org.scalacheck.{Gen, Properties}

import scala.util.Try

object SchemaRegistrySerialisation extends Properties("Confluent stack test") {

  // Schema of two records named "Foo" and "Bar"
  private val unionSchema = schemaFromResource("union-of-records.avsc")
  private val gen: Gen[GenericRecord] = genFromSchema(unionSchema)

  property("serialises with correct schema") = forAll(gen){ genericRecord =>
    val schemaRegistryClient = new MockSchemaRegistryClient()
    val config = Map(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8080",
      AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS -> true,
    )
    val serialiser = new KafkaAvroSerializer(schemaRegistryClient, CollectionConverters.toJava(config))

    // This is NOT what you want, this will post the schema for the specific branch of the union, not the union as a whole.
    val incorrectlySerialisedTopic = "wrong-topic"
    serialiser.serialize(incorrectlySerialisedTopic, genericRecord)

    // This is what you want, this will post the union schema for the topic
    val correctlySerialisedTopic = "right-topic"
    serialiser.serialize(correctlySerialisedTopic, new NonRecordContainer(unionSchema, genericRecord))

    (schemaRegistryClient.latestSchemaForTopic(incorrectlySerialisedTopic) != unionSchema) &&
      (schemaRegistryClient.latestSchemaForTopic(correctlySerialisedTopic) == unionSchema)
  }

  implicit class SchemaRegistryOps(schemaRegistryClient: SchemaRegistryClient) {
    def latestSchemaForTopic(topicName: String): Schema = {
      val metadata = schemaRegistryClient.getLatestSchemaMetadata(s"$topicName-value")
      new Schema.Parser().parse(metadata.getSchema)
    }
  }

}
