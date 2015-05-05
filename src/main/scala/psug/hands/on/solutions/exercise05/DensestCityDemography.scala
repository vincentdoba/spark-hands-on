package psug.hands.on.solutions.exercise05

import org.apache.spark.sql.{Row, SQLContext}
import psug.hands.on.exercise05.{City, DensestCityDisplayer}
import psug.hands.on.solutions.SparkContextInitiator

object DensestCityDemography extends App with SparkContextInitiator with DensestCityDisplayer with CityDemographyExtractor {

  val populationDataFile = "data/demographie_par_commune.json"

  val sparkContext = initContext("dataSetJoiner")
  val sqlContext = new SQLContext(sparkContext)

  val rawData = sqlContext.jsonFile(populationDataFile)

  val populationData = rawData
    .select("Commune","Agriculteurs", "Cadresetprofessionssupérieurs", "Employés", "Ouvriers", "Population", "Superficie")
    .where(rawData("Superficie") > 0 && rawData("Population") > 0)
    .na
    .drop()
    .map(extractDemographicData)

  val densestCity = populationData.sortBy(a => a.features.head, false).first()



  displayDensestCity(densestCity)

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
