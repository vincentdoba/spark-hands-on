package psug.hands.on.solutions.exercise02

import org.apache.spark.sql.Row
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Determine who is the winner of the 2012 French presidential election
 *
 * Déterminer qui est le gagnant de l'élection présidentielle de 2012
 *
 * File : data/resultat_presidentielles_par_commune_2012.csv
 * Fichier : data/resultat_presidentielles_par_commune_2012.csv
 *
 */
object ElectionWinner extends App with CSVLoader with SparkContextInitiator {

  val sparkContext = initContext("Winner")
  val dataFrame = getCSV("data/resultat_presidentielles_par_commune_2012.csv") // TODO not use dataframe yet...

  def getFirstCandidateResult(row: Row): (String, Integer) = getResults(row, 15, 17)
  def getSecondCandidateResult(row: Row): (String, Integer) = getResults(row, 20, 22)

  def getResults(row: Row, nameColumnIndex: Int, votesColumnIndex: Int) = (row.getString(nameColumnIndex), Integer.valueOf(row.getString(votesColumnIndex)))

  val winner = dataFrame
    .flatMap(row => Array(getFirstCandidateResult(row), getSecondCandidateResult(row)))
    .reduceByKey((a, b) => a + b)
    .sortBy(_._2, false)
    .first()._1

  println("le vainqueur de l'élection présidentielle de 2012 est " + winner)

  sparkContext.stop()

}
