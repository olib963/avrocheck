package io.github.olib963.avrocheck

import scala.jdk.CollectionConverters._

private [avrocheck] object CollectionConverters {

  def toJava[A](seq: Seq[A]): java.util.List[A] = seq.asJava
  def toJava[K, V](map: Map[K, V]): java.util.Map[K, V] = map.asJava
  def toScala[A](list: java.util.List[A]): Seq[A] = list.asScala.toList
  def toScalaMap[K, V](map: java.util.Map[K, V]): Map[K, V] = map.asScala.toMap

}
