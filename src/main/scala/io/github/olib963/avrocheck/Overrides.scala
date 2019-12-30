package io.github.olib963.avrocheck

import org.scalacheck.Gen

sealed trait Overrides

object Overrides {
  object NoOverrides extends Overrides
  case class ConstantOverride(value: Any) extends Overrides
  case class GeneratorOverrides(value: Gen[Any]) extends Overrides
  case class KeyOverrides(overrides: Map[String, Overrides]) extends Overrides
  case class SelectedUnion(branchName: String, overrides: Overrides) extends Overrides

  // TODO other possible overrides:
  // - ArrayOverride: allow Gen[Int] for size, constant collection, Gen[A] to generate elements
  // - MapOverride: allow Gen[Int] for size, constant map, Gen[A] to generate values, Gen[String] generate keys
}

