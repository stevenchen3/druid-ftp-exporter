import Dependencies._

lazy val commonSettings = Seq(
  name := "druid-ftp",
  version := "0.9",
  organization := "alphash.io",
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
  javaOptions in Test ++= Seq("-Xms1024m", "-Xmx2048m", "-Dconfig.resource=test.conf"),
  javaOptions in run ++= Seq("-Xms1024m", "-Xmx2048m", "-XX:+UseParallelGC", "-server"),
  resolvers += Resolver.sonatypeRepo("releases"),
  javaOptions in Universal := (javaOptions in run).value // propagate `run` settings to packaging scripts
)

lazy val root = Project(id = "druid-ftp", base = file("."))
  .enablePlugins(JavaServerAppPackaging, UniversalPlugin)
  .settings(commonSettings: _*)
  .settings(fork in run := true)
  .settings(fork in Test := true)
  .settings(coverageEnabled := true) // change to `false` when comes to packaging and distribution
  .settings(addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.patch))
  .settings(libraryDependencies ++= circeDeps)
  .settings(libraryDependencies ++= scalajDeps)
  .settings(libraryDependencies ++= logbackDeps)
  .settings(libraryDependencies ++= jodaTimeDeps)
  .settings(
    libraryDependencies ++= Seq(
      "com.lightbend.akka"  %% "akka-stream-alpakka-ftp" % "0.11",
      "co.fs2"              %% "fs2-io"                  % "0.9.7",
      "com.github.krasserm" %% "streamz-converter"       % "0.8.1",
      "commons-net"         %  "commons-net"             % "3.3",
      "io.circe"            %% "circe-yaml"              % "0.6.1",
      "com.github.scopt"    %% "scopt"                   % "3.7.0"
    )
  )
