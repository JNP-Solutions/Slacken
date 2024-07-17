name := "Slacken"

version := "0.1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

//For backwards compatibility with Java 17, when compiling on a newer JDK, the options below are needed.
//Also applies to javacOptions below.
scalacOptions ++= Seq("--feature", "-release", "17")

javacOptions ++= Seq("--release=17")

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "it.unimi.dsi" % "fastutil" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "latest.integration" % "test"

libraryDependencies += "org.scalatestplus" %% "scalacheck-1-15" % "latest.integration" % "test"

//The "provided" configuration prevents sbt-assembly from including spark in the packaged jar.
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

Compile / unmanagedResourceDirectories += { baseDirectory.value / "resources" }

//Do not run tests during the assembly task
//(Running tests manually is still recommended)
assembly / test := {}

//Do not include scala library JARs in assembly (provided by Spark)
assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

//Run tests in a separate JVM
Test / fork := true

Test / javaOptions += "-Xmx4G"

//These options are required when running tests on Java 17, as of Spark 3.3.0.
//Can safely be commented out on Java 8 or 11.
Test / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
Test / javaOptions += "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
Test / javaOptions += "--add-opens=java.base/java.io=ALL-UNNAMED"

Test / testOptions += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "1")
