ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.9"

lazy val root = (project in file("."))
  .settings(
    name := "lab_2"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
)