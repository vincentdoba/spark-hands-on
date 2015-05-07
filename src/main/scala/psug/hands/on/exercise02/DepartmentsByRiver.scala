package psug.hands.on.exercise02

/**
 * How are the departments whose name contains Seine ? And Loire ? And Garonne ? And Rhône ?
 *
 * Quels sont les départements dont le nom contient Seine ? Loire ? Garonne ? Rhône ?
 *
 * file : data/departements.txt
 * fichier : data/departements.txt
 *
 * command : sbt "run-main psug.hands.on.exercise01.DepartmentsCounter"
 * commande : sbt "run-main psug.hands.on.exercise01.DepartmentsCounter"
 */
object DepartmentsByRiver extends App {

  val inputFile = "data/departements.txt"
  val rivers = List("Seine", "Garonne", "Rhône", "Loire")

  val departmentsByRiver:Iterable[(String, Iterable[String])] = ???

  departmentsByRiver.foreach(row => println("Les départements dont le nom contient " + row._1 + " sont " + row._2.mkString(", ")))

}
