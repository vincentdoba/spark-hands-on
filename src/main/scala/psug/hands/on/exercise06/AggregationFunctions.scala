package psug.hands.on.exercise06

/**
 * Contains all functions necessary to aggregate the min and the max of each features of a feature list
 */
trait AggregationFunctions {

  /**
   * Generate the initialization value of aggregation action
   *
   * @param featuresSize the size of the features list
   * @return the init value for the aggregation action
   */
  def initValue(featuresSize:Int) = List.fill[Extremes](featuresSize)(None)

  /**
   * The reducer function of aggregation action
   *
   * @param previousExtremes the list of extreme value for each features
   * @param features the features you want to reduce with the list of extremes
   * @return a list of extremes for each features
   */
  def aggregationReducer(previousExtremes:List[Extremes], features:List[Double]):List[Extremes] = {
    def reduce(values:List[Double], extremes:List[Extremes], result:List[Extremes]):List[Extremes] = (values,extremes) match {
      case (Nil, Nil) => result.reverse
      case (x1::xs1, x2::xs2) => reduce(xs1, xs2, computeExtremes(x1, x2)::result)
      case _ => sys.error("Lists don't have same size")
    }

    reduce(features, previousExtremes, Nil)
  }

  /**
   * the merge function of aggregation action
   *
   * @param value1 the first extremes list
   * @param value2 the second extremes list
   * @return the merge of the two extremes lists
   */
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

/**
 * Extremes object : contains the min and the max of a sequence of features
 */
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