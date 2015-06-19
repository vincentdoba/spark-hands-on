package psug.hands.on.solutions.exercise06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import psug.hands.on.exercise05.{City, DataSaver}
import psug.hands.on.exercise06.{AggregateFunctions, Normalizer}
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Normalize features retrieved in previous exercice 05 so a Machine Learning algorithm can swallow them and save it in
 * a file
 *
 * input file : data/cities.json
 * output file : data/normalized_cities.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise06.Normalization"
 *
 */
object Normalization extends App with SparkContextInitiator with AggregateFunctions with DataSaver with Normalizer {

  val inputFile = "data/cities.json"
  val outputFile = "data/normalized_cities.json"

  init()

  val sparkContext = initContext("normalization") // Create Spark Context
  val sqlContext = new SQLContext(sparkContext) // Create SQL Context from Spark Context

  val rawData = sqlContext.read.json(inputFile) // Load JSON file

  val cities = rawData
    .select("name","category","features") // select all columns
    .map(row => City(row.getAs[String]("name"), row.getAs[Double]("category"), row.getAs[Seq[Double]]("features").toList)) // Transform Data Frame to a RDD containing 3-tuples
    .cache() // Cache the result

  val featuresSize = cities.first().features.length // Retrieve the size of features list

  val minMaxList = cities
    .map(_.features) // Extract features from cities
    .aggregate(initValue(featuresSize))(reduce, merge) // aggregate to determine min/max of each feature

  import sqlContext.implicits._ // Import SQL implicits to get toDF transformation

  val normalizedCities:RDD[String] = cities
    .map(normalize(minMaxList)) // Normalize data using min/max of each feature
    .toDF() // Transform to Data Frame
    .toJSON // Transform Data Frame to String RDD, each String is a JSON
    .cache() // Cache the result

  normalizedCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  println("Some lines of data/normalized_cities.json : ")
  normalizedCities.take(10).foreach(println)

  sparkContext.stop() // Stop connection to Spark

}