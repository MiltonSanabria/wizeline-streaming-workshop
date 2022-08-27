name := "WZStreaming"
organization := "wizeline.streaming"
version := "0.1"
scalaVersion := "2.12.15"
autoScalaLibrary := false
val sparkVersion = "3.3.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++=sparkDependencies

lazy val root = (project in file("."))
  .settings(
    name := "spark_test"
  )
