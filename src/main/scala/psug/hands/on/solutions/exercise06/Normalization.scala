package psug.hands.on.solutions.exercise06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import psug.hands.on.exercise05.{City, DataSaver}
import psug.hands.on.exercise06.{Normalizer, AggregationFunctions, Extremes}
import psug.hands.on.solutions.SparkContextInitiator
import psug.hands.on.solutions.exercise05.CityDemographyExtractor

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
object Normalization extends App with SparkContextInitiator with CityDemographyExtractor with AggregationFunctions with DataSaver with Normalizer {

  val inputFile = "data/cities.json"
  val outputFile = "data/normalized_cities.json"

  init()

  val sparkContext = initContext("normalization")
  val sqlContext = new SQLContext(sparkContext)

  val rawData = sqlContext.jsonFile(inputFile)

  val cities = rawData
    .select("name","category","features")
    .map(row => City(row.getString(0), row.getDouble(1), row.getSeq(2).toList))
    .cache()

  val featuresSize = cities.first().features.length

  val minMaxList = cities
    .map(_.features)
    .aggregate(initValue(featuresSize))(aggregationReducer, aggregationMerger)

  import sqlContext.implicits._

  val normalizedCities:RDD[String] = cities
    .map(normalize(minMaxList))
    .toDF()
    .toJSON
    .cache()

  normalizedCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  println("Some lines of data/normalized_cities.json : ")
  normalizedCities.take(10).foreach(println)

  sparkContext.stop()

}