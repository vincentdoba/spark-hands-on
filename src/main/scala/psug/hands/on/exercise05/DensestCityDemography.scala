package psug.hands.on.exercise05

/**
 * Find the densest city in France, and display the following information : 
 * - City name
 * - City density
 * - If the city has more than 5000 inhabitants
 * - Percentage of executives
 * - Percentage of employees
 * - Percentage of workers
 * - Percentage of farmers
 *
 * file : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.exercise05.DensestCityDemography"
 *
 */
object DensestCityDemography extends App  with DensestCityDisplayer {

  val inputFile = "data/demographie_par_commune.json"

  val densestCity:City = ??? // TODO extract densest city in France and its characteristics (name, has more than 5000 inhabitants...)

  displayDensestCity(densestCity)

}
