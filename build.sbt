name := "Slacken"

version := "2.0.1_sl"

lazy val scala212 = "2.12.20"

lazy val scala213 = "2.13.15"

lazy val supportedScalaVersions = List(scala212, scala213)

ThisBuild / scalaVersion := scala212

lazy val root = (project in file(".")).
  settings(
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 13)) => List("org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.0")
        case _                       => Nil
      }
    }
    )

val sparkVersion = "3.5.0"

scalacOptions ++= Seq("--feature", "-release", "17")

javacOptions ++= Seq("--release=17")

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

resolvers += "Atgenomix GitHub repo" at "https://maven.pkg.github.com/atgenomix/_"

credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "_",
  sys.env("GITHUB_TOKEN")
)

libraryDependencies += "org.rogach" %% "scallop" % "latest.integration"

libraryDependencies += "it.unimi.dsi" % "fastutil" % "latest.integration"

libraryDependencies += "org.scalatest" %% "scalatest" % "latest.integration" % "test"

libraryDependencies += "org.scalatestplus" %% "scalacheck-1-18" % "latest.integration" % "test"

libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.13.0"

//The "provided" configuration prevents sbt-assembly from including spark in the packaged jar.
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

//For Windows, to remove the dependency on winutils.exe for local filesystem access
libraryDependencies += "com.globalmentor" % "hadoop-bare-naked-local-fs" % "latest.integration"

libraryDependencies += "com.atgenomix.seqslab.plugins" % "seqslab-operator-plugin-api" % "3.1.4"

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

Test / javaOptions ++= Seq("-Xmx4G", "-Dfile.encoding=UTF-8")

//These options are required when running tests on Java 17, as of Spark 3.3.0.
//Can safely be commented out on Java 8 or 11.
Test / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
Test / javaOptions += "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
Test / javaOptions += "--add-opens=java.base/java.io=ALL-UNNAMED"

Test / testOptions += Tests.Argument(TestFrameworks.ScalaCheck, "-verbosity", "1")
