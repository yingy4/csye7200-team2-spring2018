

organization := "csye7200-team2"

logLevel := Level.Error

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"


// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"


// https://mvnrepository.com/artifact/com.mashape.unirest/unirest-java
libraryDependencies += "com.mashape.unirest" % "unirest-java" % "1.4.9"

// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
//libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.3.6"


// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpasyncclient
//libraryDependencies += "org.apache.httpcomponents" % "httpasyncclient" % "4.0.2"

// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpmime
//libraryDependencies += "org.apache.httpcomponents" % "httpmime" % "4.3.6"


// https://mvnrepository.com/artifact/org.json/json
//libraryDependencies += "org.json" % "json" % "20140107"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("com", "mashape", xs @ _*) => MergeStrategy.last
  case PathList("com", "sun", xs @ _*) => MergeStrategy.last
  case PathList("org", "glassfish", xs @ _*) => MergeStrategy.last
  case PathList("com", "amazonaws", xs @ _*) => MergeStrategy.rename
  case PathList("net", "java", xs @ _*) => MergeStrategy.rename
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "git.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}