name := "PSUG-hands-on-spark"

version := "0.0.1"

scalaVersion := "2.11.6"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.3.1" % "provided",
  "com.databricks" % "spark-csv_2.11" % "1.0.3"
)
