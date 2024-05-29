name := "RealEstateAnalysis"

version := "0.1"

scalaVersion := "2.13.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.14.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.14.1",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.14.1"
)