name := "avrocheck"
organization := "io.github.olib963"

version := "0.1.0"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.14.0",
  "org.apache.avro" % "avro" % "1.8.2",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)
