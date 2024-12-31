//> using scala 3.6.2

//> using dep io.github.quafadas::scautable::0.0.11-15-4fee12-DIRTYf22e85e5
//> using options -experimental -language:experimental.namedTuples

import io.github.quafadas.scautable.scautable.*
import io.github.quafadas.scautable.CSV
import io.github.quafadas.scautable.CSV.*
import NamedTuple.*

@main def titanic =

  def csv = CSV.absolutePath("/Users/simon/Code/titanic/titanic.csv")

  def csvDropHeader = csv.drop(1)

  val data = csvDropHeader
    .addColumn["SurvivedB", Boolean](_.Survived == "1")

  val survived = csvDropHeader
    .column["Survived"]
    .toArray
    .groupMapReduce(identity)(_ => 1)(_ + _)

  val sex = csvDropHeader
    .column["Sex"]
    .toArray
    .groupMapReduce(identity)(_ => 1)(_ + _)
    .toList

  val age = csvDropHeader
    .column["Age", Option[Double]](_.toDoubleOption)
    .toArray
    .groupMapReduce(identity)(_ => 1)(_ + _)
    .toList

  val group =
    csvDropHeader
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
    s"survived $survived : % of total ${survived.get("1").sum.toDouble / csvDropHeader.size.toDouble}"
  )
  println(sex.consoleShow)

  println(group.consolePrint())
