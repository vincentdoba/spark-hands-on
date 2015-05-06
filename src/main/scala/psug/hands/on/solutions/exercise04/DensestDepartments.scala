package psug.hands.on.solutions.exercise04

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, LongType}
import psug.hands.on.solutions.SparkContextInitiator

/**
 * How are the densest department ?
 *
 * Quels sont les départements les plus peuplés ?
 *
 * file : data/departements.txt, data/demographie_par_commune.json
 * fichier : data/departements.txt, data/demographie_par_commune.json
 */
object DensestDepartments extends App with SparkContextInitiator {

  val departmentsFile = "data/departements.txt"
  val dataFile = "data/demographie_par_commune.json"

  val sparkContext = initContext("densestDepartment")
  val sqlContext = new SQLContext(sparkContext)

  val data = sqlContext.jsonFile(dataFile)

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val densestDepartmentsCodes = data
    .filter("Population > 0")
    .filter("Superficie > 0")
    .select("Departement", "Population", "Superficie")
    .groupBy("Departement")
    .agg($"Departement", sum("Population").as("Population"), sum("Superficie").as("Superficie"))
    .select($"Departement", ($"Population" / $"Superficie").cast(DoubleType))
    .map(row => (row.getString(0), row.getDouble(1)))
  
  val departments = sparkContext.textFile(departmentsFile)

  val departmentsNamesByCode = departments
    .map(line => line.split(","))
    .map(a => (a(1), a(0)))

  val densestDepartmentsNames = densestDepartmentsCodes
    .join(departmentsNamesByCode)
    .sortBy(_._2._1, false)
    .values
    .map(_._2)
    .take(10)

  println("Les départements les plus densément peuplés sont " + densestDepartmentsNames.mkString(", "))

  sparkContext.stop()


}
