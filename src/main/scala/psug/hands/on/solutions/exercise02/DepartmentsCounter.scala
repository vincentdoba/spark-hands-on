package psug.hands.on.solutions.exercise02

import psug.hands.on.solutions.SparkContextInitiator

/**
 * How many departements are there ?
 *
 * Combien y-a-t-il de département ?
 *
 * file : data/departements.txt
 * fichier : data/departements.txt
 *
 * command : sbt "run-main psug.hands.on.solutions.exercise02.DepartmentsCounter"
 * commande : sbt "run-main psug.hands.on.solutions.exercise02.DepartmentsCounter"
 */
object DepartmentsCounter extends App with SparkContextInitiator {

  val inputFile = "data/departements.txt"

  val sparkContext = initContext("densestDepartment")

  val input = sparkContext.textFile(inputFile)

  val numberOfDepartments = input.count()

  println("Il y a " + numberOfDepartments + " départements")

  sparkContext.stop()

}
