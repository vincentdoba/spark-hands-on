package psug.hands.on.exercise05

/**
 * Represent a City
 *
 * @param name name of the City, for instance Paris
 * @param category category of the city, for instance 1, 3,... Meaning of this field depends on user
 * @param features features, Meaning of elements of the list depends on user
 */
case class City(name:String, category:Double, features: List[Double])
