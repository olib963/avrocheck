package io.github.olib963.avrocheck

import org.scalacheck.Gen

import scala.reflect.ClassTag

sealed trait Overrides

private[avrocheck] object Overrides {
  object NoOverrides extends Overrides
  case class ConstantOverride(value: Any) extends Overrides
  case class KeyOverrides(overrides: Map[String, Overrides]) extends Overrides
  case class SelectedUnion(branchName: String, overrides: Overrides) extends Overrides

  class GeneratorOverrides[A] private (val generator: Gen[A], val classTag: ClassTag[A]) extends Overrides {
    override def toString: String = s"GeneratorOverrides(type = $classTag, generator = $generator)"
  }
  object GeneratorOverrides {
    def apply[A](generator: Gen[A])(implicit classTag: ClassTag[A]): GeneratorOverrides[A] = new GeneratorOverrides(generator, classTag)
    def unapply(arg: GeneratorOverrides[_]): Option[(Gen[Any], ClassTag[_ <: Any])] = Some((arg.generator, arg.classTag))
  }

  // TODO other possible overrides:
  // - ArrayOverride: allow Gen[Int] for size, constant collection, Gen[A] to generate elements
  // - MapOverride: allow Gen[Int] for size, constant map, Gen[A] to generate values, Gen[String] generate keys
}

