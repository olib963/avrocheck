package io.github.olib963.avrocheck

object ScalaVersionSpecificOrderings {
  implicit val doubleOrder: Ordering[Double] = Ordering.Double.TotalOrdering
  implicit val floatOrdering: Ordering[Float] = Ordering.Float.TotalOrdering
}
