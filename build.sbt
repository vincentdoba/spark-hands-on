name := "PSUG-hands-on-spark"

version := "0.0.1"

scalaVersion := "2.10.5"

resolvers += Resolver.mavenLocal

fork := true

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0",
  "org.apache.spark" %% "spark-sql" % "1.4.0",
  "org.apache.spark" %% "spark-mllib" % "1.4.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" exclude("javax.servlet", "*")
)
