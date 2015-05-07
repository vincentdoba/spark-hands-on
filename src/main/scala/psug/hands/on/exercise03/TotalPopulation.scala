package psug.hands.on.exercise03

/**
 * Determine the total population in France
 *
 * Déterminer la population totale de la France
 *
 * file : data/demographie_par_commune.json
 * fichier : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.exercise03.TotalPopulation"
 * commande : sbt "run-main psug.hands.on.exercise03.TotalPopulation"
 */
object TotalPopulation extends App {

  val inputFile = "data/demographie_par_commune.json"

  val population:Long = ??? // TODO extract total population in France in 2010

  println("La France compte " + population + " habitants")



}
