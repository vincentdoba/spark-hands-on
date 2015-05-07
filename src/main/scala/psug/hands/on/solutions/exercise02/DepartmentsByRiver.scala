package psug.hands.on.solutions.exercise02

import psug.hands.on.solutions.SparkContextInitiator

/**
 * How are the departments whose name contains Seine ? And Loire ? And Garonne ? And Rhône ?
 *
 * Quels sont les départements dont le nom contient Seine ? Loire ? Garonne ? Rhône ?
 *
 * file : data/departements.txt
 * fichier : data/departements.txt
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise02.DepartmentsByRiver"
 * commande : sbt "run-main psug.hands.on.solutions.exercise02.DepartmentsByRiver"
 */
object DepartmentsByRiver extends App with SparkContextInitiator {

  val inputFile = "data/departements.txt"
  val rivers = List("Seine", "Garonne", "Rhône", "Loire")

  val sparkContext = initContext("densestDepartment")

  val input = sparkContext.textFile(inputFile)


  val departmentsByRiver:Iterable[(String, Iterable[String])] = input
    .flatMap {
    case a if a.split(",")(0).contains("Seine") => List(("Seine", a.split(",")(0)))
    case a if a.split(",")(0).contains("Rhône") => List(("Rhône", a.split(",")(0)))
    case a if a.split(",")(0).contains("Loire") => List(("Loire", a.split(",")(0)))
    case a if a.split(",")(0).contains("Garonne") => List(("Garonne", a.split(",")(0)))
    case _ => List()
  }
    .groupByKey()
    .map(a => (a._1, a._2))
    .sortByKey()
    .collect()

  departmentsByRiver.foreach(row => println("Les départements dont le nom contient " + row._1 + " sont " + row._2.mkString(", ")))

  sparkContext.stop()

}
