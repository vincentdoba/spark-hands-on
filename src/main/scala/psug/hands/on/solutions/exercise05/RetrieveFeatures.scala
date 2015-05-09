package psug.hands.on.solutions.exercise05

import org.apache.spark.sql.{Row, SQLContext}
import psug.hands.on.exercise05.{DataSaver, City}
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Save the following information in a JSON and display the following information :
 * 
 * - City name
 * - City density
 * - If the city has more than 5000 inhabitants
 * - Percentage of executives
 * - Percentage of employees
 * - Percentage of workers
 * - Percentage of farmers
 * 
 *
 * file : data/demographie_par_commune.json
 * output file : data/cities.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise05.RetrieveFeatures"
 *
 */
object RetrieveFeatures extends App with SparkContextInitiator with CityDemographyExtractor with DataSaver {

  val inputFile = "data/demographie_par_commune.json"
  val outputFile = "data/cities.json"

  init()

  val sparkContext = initContext("retrieveFeatures")
  val sqlContext = new SQLContext(sparkContext)

  val rawData = sqlContext.jsonFile(inputFile)

  val cities = rawData
    .select("Commune","Agriculteurs", "Cadresetprofessionssupérieurs", "Employés", "Ouvriers", "Population", "Superficie")
    .where(rawData("Superficie") > 0 && rawData("Population") > 2000)
    .na
    .drop()
    .map(extractDemographicData)

  import sqlContext.implicits._
  cities
    .toDF()
    .toJSON
    .saveAsTextFile(temporaryFile)

  merge(temporaryFile, outputFile)

  sparkContext.stop()

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