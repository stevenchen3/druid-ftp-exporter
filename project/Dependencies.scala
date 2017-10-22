import sbt._

object Dependencies {

  // Circe library dependencies
  lazy val circeVersion = "0.9.0-M1"
  val circeDeps = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-generic-extras",
    "io.circe" %% "circe-optics",
    "io.circe" %% "circe-parser",
    "io.circe" %% "circe-literal",
    "io.circe" %% "circe-fs2"
  ).map(_ % circeVersion)

  val scalajDeps = Seq(
    "org.scalaj" %% "scalaj-http" % "1.1.5"
  )

  val jodaTimeDeps = Seq(
    "joda-time" % "joda-time" % "2.9.9"
  )
}
