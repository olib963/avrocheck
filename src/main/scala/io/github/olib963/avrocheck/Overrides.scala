package io.github.olib963.avrocheck

sealed trait Overrides

object Overrides {
  object NoOverrides extends Overrides
  case class ConstantOverride(value: Any) extends Overrides
  case class KeyOverrides(overrides: Map[String, Overrides]) extends Overrides
  case class SelectedUnion(branchName: String, overrides: Overrides) extends Overrides

  // TODO other possible overrides:
  // - GeneratorOverride[A](gen: Gen[A]) to override the gen for one field, need to make sure it is type safe
  // - ArrayOverride: allow Gen[Int] for size, constant collection, Gen[A] to generate elements
  // - MapOverride: allow Gen[Int] for size, constant map, Gen[A] to generate values, Gen[String] generate keys
}

