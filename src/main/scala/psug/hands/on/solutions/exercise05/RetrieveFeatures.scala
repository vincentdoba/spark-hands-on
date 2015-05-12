package psug.hands.on.solutions.exercise05

import org.apache.spark.sql.{Row, SQLContext}
import psug.hands.on.exercise05.{City, DataSaver}
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Save the following information in a JSON file :
 *
 * - City name
 * - If the city has more than 5000 inhabitants
 * - City density
 * - Percentage of executives
 * - Percentage of employees
 * - Percentage of workers
 * - Percentage of farmers
 *
 *
 * input file : data/demographie_par_commune.json
 * output file : data/cities.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise05.RetrieveFeatures"
 *
 */
object RetrieveFeatures extends App with SparkContextInitiator with CityDemographyExtractor with DataSaver {

  val inputFile = "data/demographie_par_commune.json"
  val outputFile = "data/cities.json"

  init()

  val sparkContext = initContext("retrieveFeatures") // Create Spark Context
  val sqlContext = new SQLContext(sparkContext) // Create SQL Context from Spark Context

  val rawData = sqlContext.jsonFile(inputFile) // Load JSON File into a Data Frame

  import sqlContext.implicits._ // Load SQL implicites which wil  allow us to use toDF Transformation

  val cities = rawData
    .select("Commune", "Agriculteurs", "Cadresetprofessionssupérieurs", "Employés", "Ouvriers", "Population", "Superficie") // Select interesting columns
    .where(rawData("Superficie") > 0 && rawData("Population") > 2000) // Filter by surface and population
    .na // Select rows containing null value
    .drop() // And drop them
    .map(extractDemographicData) // Compute demographic data and save it in a City object, transform data frame to RDD
    .toDF() // Transform to Data Frame in order to use toJSON
    .toJSON // Transform each row into a JSON String, transform again to a RDD
    .cache() // Save into memory, in order to not compute all RDD again

  cities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  println("Some lines of data/cities.json : ")
  cities.take(10).foreach(println)

  sparkContext.stop() // Stop connection to Spark

}

trait CityDemographyExtractor {

  def extractDemographicData(row: Row) = {
    val name = row.getString(0)
    val population = row.getLong(5)
    val farmerPercentage = row.getLong(1)*100/population
    val executivePercentage = row.getLong(2)*100/population
    val employeePercentage = row.getLong(3)*100/population
    val workerPercentage = row.getLong(4)*100/population
    val density = population / row.getLong(6)
    val hasMoreThanFiveThousandInhabitants = if (population > 5000) 1 else 0
    City(name, hasMoreThanFiveThousandInhabitants, List(density, executivePercentage, employeePercentage, workerPercentage, farmerPercentage))
  }

}
