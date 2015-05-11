package psug.hands.on.exercise06

import psug.hands.on.exercise05.City

trait Normalizer {

  /**
   * Normalize a city's features list given a base list of extremes
   *
   * @param base the base list, same length of the features list of the city, containing min and max for each feature
   * @param city the city you want the feature to be normalized
   * @return a city with features normalized
   */
  def normalize(base: List[Extremes])(city:City):City = {
    def normalizeInner(features:List[Double], extremes:List[Extremes], result:List[Double]):List[Double] = (features, extremes) match {
      case (Nil, Nil) => result.reverse
      case (x1::xs1, x2::xs2) => normalizeInner(xs1, xs2, normalizeFeature(x1, x2)::result)
      case _ => sys.error("Lists don't have same size")
    }

    val normalizedFeatures = normalizeInner(city.features, base, Nil)
    City(city.name, city.category, normalizedFeatures)

  }

  private def normalizeFeature(feature:Double, extreme:Extremes):Double = extreme match {
    case None => feature
    case Some(min, max) => round((feature - min)/(max - min))
  }

  private def round(value:Double):Double = (math rint (value*100))/100

}
