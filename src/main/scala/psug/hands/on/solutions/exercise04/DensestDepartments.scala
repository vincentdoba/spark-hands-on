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

  val sparkContext = initContext("densestDepartment") // Create Spark Context
  val sqlContext = new SQLContext(sparkContext) // Create SQL Context from Spark Context

  val data = sqlContext.read.json(inputFile) // Load JSON File into a Data Frame

  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  val densestDepartmentsCodes = data
    .filter("Population > 0") // Filter by population > 0
    .filter("Superficie > 0") // Filter by surface > 0
    .select("Departement", "Population", "Superficie") // Select the three interesting columns : Department, Population and Surface
    .groupBy("Departement") // Group by departments, agg function below will define how we merge other columns values
    .agg($"Departement", sum("Population").as("Population"), sum("Superficie").as("Superficie")) // Define how to merge values/ what we want to retrieve as columns
    .select($"Departement", ($"Population" / $"Superficie").cast(DoubleType).as("Density")) // Compute density by dividing the two columns Population and Surface
    .map(row => (row.getAs[String]("Departement"), row.getAs[Double]("Density"))) // Transform Data Frame to RDD with the information of the two remaining columns
  
  val departments = sparkContext.textFile(departmentsFile) // Load departement.txt into a RDD

  val departmentsNamesByCode = departments
    .map(line => line.split(",")) // Transform String into list of String
    .map(a => (a(1), a(0))) // Transform RDD to Pair RDD

  val densestDepartments:Iterable[String] = densestDepartmentsCodes
    .join(departmentsNamesByCode) // Merge the two RDD : the one with the density and the one with the name of the department
    .sortBy(_._2._1, false) // Sort by density, in descending order
    .values // Get Values of Key/Value RDD, discard Keys
    .map(_._2) // Extract Name of the departments
    .take(10) // Take the 10 first departments' names

  println("Les départements les plus densément peuplés sont " + densestDepartments.mkString(", "))

  sparkContext.stop() // Stop connection to Spark


}
