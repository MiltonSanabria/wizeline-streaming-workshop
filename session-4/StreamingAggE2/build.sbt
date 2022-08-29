ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "streaming-multidim-aggregation"
  )

val sparkVersion = "3.3.0"

//libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10_2.13" % sparkVersion % "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.2", //provided removed
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.2", //provided removed
  "org.apache.kafka" % "kafka-clients" % "3.2.1"
)