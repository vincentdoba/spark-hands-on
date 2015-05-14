package psug.hands.on.exercise04

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Which are the densest departments ?
 *
 * input file 1 : data/departements.txt
 * input file 2 : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.exercise04.DensestDepartments"
 */
object DensestDepartments extends App {

  val departmentsFile = "data/departements.txt"
  val inputFile = "data/demographie_par_commune.json"

  val densestDepartments:Iterable[String] = Nil // TODO Create an iterable ("densestDepartment","secondDensestDepartment",...)

  println("Les départements les plus densément peuplés sont " + densestDepartments.mkString(", "))

}
