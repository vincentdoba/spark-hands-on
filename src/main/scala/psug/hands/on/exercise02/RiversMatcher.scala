package psug.hands.on.exercise02

/**
 * Helper to match department with river name contained in it
 */
trait RiversMatcher {

  /**
   * Match department name with the rivers it contains. If there is no river in department name, return empty option
   *
   * @param departmentName the name of the department, for instance "Seine-Saint-Denis"
   * @return an option containing tuple with the name of the river and the name of the department, for instance ("Seine","Seine-Saint-Denis")
   *         if there is no match with one of the main rivers in France, return None (empty Option)
   */
  def label(departmentName:String):Option[(String, String)] = departmentName match {
    case name if name.contains("Rhône") => Some(("Rhône", name))
    case name if name.contains("Loire") => Some(("Loire", name))
    case name if name.contains("Seine") => Some(("Seine", name))
    case name if name.contains("Garonne") => Some(("Garonne", name))
    case _ => None
  }

}
