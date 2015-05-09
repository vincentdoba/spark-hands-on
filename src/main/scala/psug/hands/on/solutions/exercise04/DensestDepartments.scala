package psug.hands.on.solutions.exercise04

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Which are the densest departments ?
 *
 * input file 1 : data/departements.txt
 * input file 2 : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise04.DensestDepartments"
 */
object DensestDepartments extends App with SparkContextInitiator {

  val departmentsFile = "data/departements.txt"
  val inputFile = "data/demographie_par_commune.json"

  val sparkContext = initContext("densestDepartment")
  val sqlContext = new SQLContext(sparkContext)

  val data = sqlContext.jsonFile(inputFile)

  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

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

  val densestDepartments:Iterable[String] = densestDepartmentsCodes
    .join(departmentsNamesByCode)
    .sortBy(_._2._1, false)
    .values
    .map(_._2)
    .take(10)

  println("Les départements les plus densément peuplés sont " + densestDepartments.mkString(", "))

  sparkContext.stop()


}
