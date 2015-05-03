package psug.hands.on.solutions.exercise1

case class TownElectionResults(departmentCode:Int, townCode:Int, townName:String, firstCandidate:Candidate, secondCandidate:Candidate)

case class Candidate(name:String, numberOfVote:Int)