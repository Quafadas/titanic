//> using scala 3.6.2

//> using resourceDir data

//> using dep io.github.quafadas::scautable::0.0.11-23-3f71ff-DIRTY144c070

//> using options -experimental -language:experimental.namedTuples

import io.github.quafadas.scautable.scautable.*
import io.github.quafadas.scautable.CSV
import io.github.quafadas.scautable.CSV.*
import NamedTuple.*

enum Gender:
  case Male, Female

@main def titanic =

  def csv = CSV.resource("titanic.csv")

  def data = csv
    .mapColumn["Sex", Gender]((x: String) => Gender.valueOf(x.capitalize))
    .dropColumn["PassengerId"]

  val survived = data
    .column["Survived"]
    .toArray
    .groupMapReduce(identity)(_ => 1)(_ + _)

  val sex = data
    .column["Sex"]
    .toArray
    .groupMapReduce(identity)(_ => 1)(_ + _)
    .toList

  val age = csv
    .mapColumn["Age", Option[Double]](_.toDoubleOption)
    .column["Age"]
    .toArray
    .groupMapReduce(identity)(_ => 1)(_ + _)
    .toList

  val group =
    csv.toArray
      .map(x => (x.Survived, x.Sex).withNames[("Survived", "Sex")])
      .toArray
      .groupMapReduce(_.Sex)(x => (x.Survived.toInt, 1, 0.0)) {
        case ((surviveAcc, oneAcc, percAcc), (c, d, e)) =>
          (
            surviveAcc + c,
            oneAcc + d,
            100 * (surviveAcc + c).toDouble / (oneAcc + d).toDouble
          )
      }
      .toList
      .map { case (x, (a, b, c)) =>
        (x, a, b, c).withNames[("Sex", "Survived", "CohortCount", "%")]
      }

  println(data.toArray.take(20).consolePrint())

  println(
    s"survived $survived : % of total ${survived.get("1").sum.toDouble / csv.size.toDouble}"
  )
  println(sex.consoleShow)

  println(group.consolePrint())
