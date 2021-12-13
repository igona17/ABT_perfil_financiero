name := "abt-bancos-zona-qua"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq("org.scala-lang" % "scala-library" % "2.11.12",
  "org.scalatest" %% "scalatest" % "3.0.8",
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" % "2.4.0"
)