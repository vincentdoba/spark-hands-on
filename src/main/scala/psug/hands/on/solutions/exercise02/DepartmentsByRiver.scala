package psug.hands.on.solutions.exercise02

import psug.hands.on.solutions.SparkContextInitiator

/**
 * How are the departments whose name contains Seine ? And Loire ? And Garonne ? And Rhône ?
 *
 * input file : data/departements.txt
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise02.DepartmentsByRiver"
 */
object DepartmentsByRiver extends App with SparkContextInitiator {

  val inputFile = "data/departements.txt"
  val rivers = List("Seine", "Garonne", "Rhône", "Loire")

  val sparkContext = initContext("densestDepartment") // Create the Spark Context

  val input = sparkContext.textFile(inputFile) // Load the file departments.txt in an RDD


  val departmentsByRiver:Iterable[(String, String)] = input
    .flatMap {
    case a if a.split(",")(0).contains("Seine") => List(("Seine", a.split(",")(0)))
    case a if a.split(",")(0).contains("Rhône") => List(("Rhône", a.split(",")(0)))
    case a if a.split(",")(0).contains("Loire") => List(("Loire", a.split(",")(0)))
    case a if a.split(",")(0).contains("Garonne") => List(("Garonne", a.split(",")(0)))
    case _ => List()
  } // Create a Key/Value RDD ("River","Department Name"), without null value
    .reduceByKey((a, b) => a + ", " + b)  // Merge departement name by river name
    .sortByKey() // Sort by river name
    .collect() // Retrieve all elements of the RDD

  departmentsByRiver.foreach(row => println("Les départements dont le nom contient " + row._1 + " sont " + row._2))

  sparkContext.stop() // Stop connection to spark

}
