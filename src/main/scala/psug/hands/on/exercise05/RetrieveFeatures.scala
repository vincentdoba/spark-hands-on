package psug.hands.on.exercise05

/**
 * Save the following information in a JSON and display the following information :
 *
 * - City name
 * - City density
 * - If the city has more than 5000 inhabitants
 * - Percentage of executives
 * - Percentage of employees
 * - Percentage of workers
 * - Percentage of farmers
 *
 *
 * file : data/demographie_par_commune.json
 * output file : data/cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise05.RetrieveFeatures"
 *
 */
object RetrieveFeatures extends App  with DataSaver {

  val inputFile = "data/demographie_par_commune.json"
  val outputFile = "data/cities.json"

  init()

  // TODO save cities in France and their characteristics (name, has more than 5000 inhabitants...) to temporaryFile

  merge(temporaryFile, outputFile)

}
