package io.github.olib963.avrocheck

import scala.collection.JavaConverters._

private [avrocheck] object CollectionConverters {

  def toJava[A](seq: Seq[A]): java.util.List[A] = seq.asJava
  def toJava[K, V](map: Map[K, V]): java.util.Map[K, V] = map.asJava
  def toScala[A](list: java.util.List[A]): Seq[A] = list.asScala

}
