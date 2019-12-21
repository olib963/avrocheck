package io.github.olib963.avrocheck

import scala.io.Source

object SourceReader {

  def readSource(sourceFile: String): String = Source.fromResource(sourceFile).mkString

}
