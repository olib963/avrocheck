name := "avrocheck"
organization := "io.github.olib963"

version := "0.1.0"

val scala13 = "2.13.1"
val scala12 = "2.12.10"

scalaVersion := scala12
val scalacheckVersion = "1.14.0"

// Snapshot repo used for test framework. Remove this when 0.3.0 is released
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % scalacheckVersion,
  "org.apache.avro" % "avro" % "1.8.2",
  "com.sksamuel.avro4s" %% "avro4s-core" % "3.0.4" % Test
)

javatestScalacheckVersion := Some(scalacheckVersion)

scalacOptions ++= Seq(
  // Fail build on warnings
  "-unchecked", "-deprecation", "-feature", "-Xfatal-warnings",

  // Enable higher kinded types
  "-language:higherKinds"
)

crossScalaVersions := Seq(scala12, scala13)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/olib963/avrocheck"),
    "scm:git@github.com:olib963/avrocheck.git"
  )
)

developers := List(
  Developer(
    id = "olib963",
    name = "olib963",
    email = "",
    url = url("https://github.com/olib963")
  )
)

licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/olib963/avrocheck"))

publishTo := sonatypePublishToBundle.value

Test / parallelExecution := false
