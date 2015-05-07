package psug.hands.on.exercise05

/**
 * Find the densest city in France, and display the following information : 
 * <ul>
 *   <li>City name</li>
 *   <li>City density</li>
 *   <li>If the city has more than 5000 inhabitants</li>
 *   <li>Percentage of executives</li>
 *   <li>Percentage of employees</li>
 *   <li>Percentage of workers</li>
 *   <li>Percentage of farmers</li>
 * </ul>
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

trait DensestCityDisplayer {
  
  def displayDensestCity(densestCity:City) {
    val townName = densestCity.name
    val hasMoreThanFiveThousandsInhabitants = if (densestCity.category > 0) "plus" else "moins"
    val density = densestCity.features(0)
    val executivePercentage = densestCity.features(1)
    val employeePercentage = densestCity.features(2)
    val workerPercentage = densestCity.features(3)
    val farmerPercentage = densestCity.features(4)

    println(s"La ville la plus densément peuplée de France est $townName avec une densité de $density habitants par km², \n" +
      s"elle a $hasMoreThanFiveThousandsInhabitants de 5000 habitants, \n" +
      s"et est constituée à $executivePercentage % de cadres, $employeePercentage % d'employés, $workerPercentage % d'ouvriers et $farmerPercentage % de fermiers")
  }
}
