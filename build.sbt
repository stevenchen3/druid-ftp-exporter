import Dependencies._

lazy val commonSettings = Seq(
  name         := "druid-ftp-exporter",
  version      := "0.9.0-SNAPSHOT",
  organization := "io.alphash",
  scalaVersion := "2.11.8",
  scalacOptions in Compile ++= Seq(
    "-encoding",
    "UTF-8",
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlog-reflective-calls",
    "-Xlint",
    "-Yrangepos",
    "-language:higherKinds",
    "-language:implicitConversions"
  ),
  javacOptions in Compile ++= Seq(
    "-source",
    "1.8",
    "-target",
    "1.8",
    "-Xlint:unchecked",
    "-Xlint:deprecation"
  ),
  javaOptions in Test ++= Seq("-Xms256m", "-Xmx2g", "-Dconfig.resource=test.conf"),
  javaOptions in run  ++= Seq("-Xms256m", "-Xmx2g", "-XX:+UseParallelGC", "-server"),
  resolvers += Resolver.sonatypeRepo("releases")
)

lazy val root = Project(id = "druid-ftp-exporter", base = file("."))
  .settings(commonSettings: _*)
  .settings(fork in run     := true)
  .settings(fork in Test    := true)
  .settings(coverageEnabled := true)
  .settings(addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.patch))
  .settings(libraryDependencies ++= circeDeps)
  .settings(libraryDependencies ++= scalajDeps)
  .settings(libraryDependencies ++= jodaTimeDeps)
  .settings(libraryDependencies ++= Seq(
      "com.lightbend.akka"  %% "akka-stream-alpakka-ftp" % "0.11",
      "co.fs2"              %% "fs2-io"                  % "0.9.7",
      "com.github.krasserm" %% "streamz-converter"       % "0.8.1",
      "commons-net"         %  "commons-net"             % "3.3",
      "io.circe"            %% "circe-yaml"              % "0.6.1",
      "com.github.scopt"    %% "scopt"                   % "3.7.0"
    )
  )
