package io.github.olib963.avrocheck

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.mutable

private [avrocheck] class GenericRecordBuilder(schema: Schema) extends mutable.Builder[(String, Any), GenericRecord] {
  var record = new GenericData.Record(schema)

  override def addOne(elem: (String, Any)): this.type = {
    record.put(elem._1, elem._2)
    this
  }

  override def clear(): Unit = record = new GenericData.Record(schema)

  override def result(): GenericRecord = record
}
