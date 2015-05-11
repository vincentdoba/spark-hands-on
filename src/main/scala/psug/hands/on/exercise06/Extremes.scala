package psug.hands.on.exercise06

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
