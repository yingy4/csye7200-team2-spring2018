scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0"

)

// https://mvnrepository.com/artifact/master/spark-stemming
libraryDependencies += "master" % "spark-stemming" % "0.2.0"


organization := "csye7200-team2"

logLevel := Level.Debug

