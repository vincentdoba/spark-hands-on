package psug.hands.on.exercise04

/**
 * How are the ten densest department ?
 *
 * Quels sont les dix départements les plus densement peuplés ?
 *
 * file : data/departements.txt, data/demographie_par_commune.json
 * fichier : data/departements.txt, data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.exercise04.DensestDepartments"
 * commande : sbt "run-main psug.hands.on.exercise04.DensestDepartments"
 */
object DensestDepartments extends App {

  val departmentsFile = "data/departements.txt"
  val inputFile = "data/demographie_par_commune.json"

  val densestDepartments:Iterable[String] = Nil // TODO Create an iterable ("densestDepartment","secondDensestDepartment",...)

  println("Les départements les plus densément peuplés sont " + densestDepartments.mkString(", "))

}
