name := "avrocheck"
organization := "io.github.olib963"

version := "0.1.0"

scalaVersion := "2.12.8"
val scalacheckVersion = "1.14.0"

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % scalacheckVersion,
  "org.apache.avro" % "avro" % "1.8.2"
)

javatestScalacheckVersion := Some(scalacheckVersion)
