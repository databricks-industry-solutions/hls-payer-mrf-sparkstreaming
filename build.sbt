name := "payer-mrf-streamsource"

version := "0.2"

lazy val scala212 = "2.12.8"
lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.2.1")
ThisBuild / organization := "com.databricks.labs"
ThisBuild / organizationName := "Databricks, Inc."

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0"
).map(_ % " provided")

lazy val testDependencies = Seq("org.scalatest" %% "scalatest" % "3.2.14" % Test)

val coreDependencies = Seq(
  "com.google.guava" % "guava" % "12.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.0"
)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ coreDependencies

assemblyJarName := s"${name.value}-${version.value}_assembly.jar"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${name.value}-${version.value}." + artifact.extension
}

