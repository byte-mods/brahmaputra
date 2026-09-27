// sbt build for the Scala wrapper. The Java driver is compiled from ../java/src/main/java
// as part of this project (mixed compilation; nothing is copied), so the jar carries both.
//
//   sbt package                                                   # target/scala-3.3.8/*.jar
//   sbt "Test/runMain io.brahmaputra.scaladsl.ManualTest 127.0.0.1 9092"
//
// ./test.sh does the same with nothing but a JDK and curl.

ThisBuild / organization := "io.brahmaputra"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "3.3.8"

lazy val root = (project in file("."))
  .settings(
    name := "brahmaputra-scala",
    Compile / unmanagedSourceDirectories +=
      baseDirectory.value / ".." / "java" / "src" / "main" / "java",
    compileOrder := CompileOrder.Mixed,
    javacOptions ++= Seq("--release", "17", "-Xlint:all"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-Werror", "-release", "17"),
    // ManualTest calls sys.exit with its verdict, so it runs in its own JVM.
    Test / run / fork := true
  )
