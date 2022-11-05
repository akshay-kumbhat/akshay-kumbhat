ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "spark-l1"
  )

libraryDependencies += "org.apache.spark" % "spark-sql_2.13" % "3.3.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.13" % "3.3.0"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.13.8"