package psug.hands.on.solutions.exercise01

import psug.hands.on.solutions.exercise04.DensestDepartments._

/**
 * How many departements are there ?
 *
 * Combien y-a-t-il de département ?
 *
 * file : data/departements.txt
 * fichier : data/departements.txt
 */
object DepartmentsCounter extends App {

  val inputFile = args(0)

  val sparkContext = initContext("densestDepartment")

  val input = sparkContext.textFile(inputFile)

  val numberOfDepartments = input.count()

  println("Il y a " + numberOfDepartments + " départements")

  sparkContext.stop()

}
