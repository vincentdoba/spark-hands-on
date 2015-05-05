package psug.hands.on.solutions.exercise06

import org.apache.spark.sql.SQLContext
import psug.hands.on.exercise05.City
import psug.hands.on.solutions.SparkContextInitiator
import psug.hands.on.solutions.exercise05.CityDemographyExtractor

/**
 * Normalize features retrieved in previous exercice 05 so a Machine Learning algorithm can swallow them
 *
 * file : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.exercise06.Normalization"
 *
 */
object Normalization extends App with SparkContextInitiator with CityDemographyExtractor with Normalizer {

  val populationDataFile = "data/demographie_par_commune.json"

  val sparkContext = initContext("dataSetJoiner")
  val sqlContext = new SQLContext(sparkContext)

  val rawData = sqlContext.jsonFile(populationDataFile)

  val populationData = rawData
    .select("Commune","Agriculteurs", "Cadresetprofessionssupérieurs", "Employés", "Ouvriers", "Population", "Superficie")
    .where(rawData("Superficie") > 0 && rawData("Population") > 500)
    .na
    .drop()
    .map(extractDemographicData)
    .cache()

  val featuresSize = populationData.first().features.length

  val minMaxList = populationData
    .map(_.features)
    .aggregate(List.fill[Extremes](featuresSize)(None))(aggregationReducer, aggregationMerger)

  val normalizedData = populationData.map(normalize(minMaxList))

  val densestCityNormalized = normalizedData.sortBy(_.features.head, false).first()

  println(densestCityNormalized.features.mkString(", "))

  sparkContext.stop()

}


trait Normalizer {

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
    case Some(min, max) => (feature - min)*100/(max - min)
  }

  def aggregationReducer(previousExtremes:List[Extremes], features:List[Double]):List[Extremes] = {
    def reduce(values:List[Double], extremes:List[Extremes], result:List[Extremes]):List[Extremes] = (values,extremes) match {
      case (Nil, Nil) => result.reverse
      case (x1::xs1, x2::xs2) => reduce(xs1, xs2, computeExtremes(x1, x2)::result)
      case _ => sys.error("Lists don't have same size")
    }
    
    reduce(features, previousExtremes, Nil)
  }

  def aggregationMerger(value1:List[Extremes], value2:List[Extremes]):List[Extremes] = {
    def reduce(extremes1:List[Extremes], extremes2:List[Extremes], result:List[Extremes]):List[Extremes] = (extremes1,extremes2) match {
      case (Nil, Nil) => result.reverse
      case (x1::xs1, x2::xs2) => reduce(xs1, xs2, mergeExtremes(x1, x2)::result)
      case _ => sys.error("Lists don't have same size")
    }

    reduce(value1, value2, Nil)
  }
  
  private def computeExtremes(value:Double, previousExtreme:Extremes) = previousExtreme match {
    case None => Some(value, value)
    case Some(min, max) => if (value < min) Some(value, max) else if (value > max) Some(min, value) else Some(min, max) 
  }

  private def mergeExtremes(extremes1:Extremes, extremes2:Extremes):Extremes = (extremes1, extremes2) match {
    case (a, None) => a
    case (None, b) => b
    case (Some(min1, max1), Some(min2, max2)) => Some(Math.min(min1, min2), Math.max(max1, max2))
  }
}


sealed trait Extremes {
  def min:Double
  def max:Double
}

object None extends Extremes with Serializable {
  override def min: Double = sys.error("not defined")
  override def max: Double = sys.error("not defined")
  override def toString = "No Extremes"
}

case class Some(min:Double, max:Double) extends Extremes {
  override def toString = s"(min : $min, max : $max)"
}
