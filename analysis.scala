//> using scala 3.6.2

//> using resourceDir data

//> using dep io.github.quafadas::scautable:0.0.11-25-eb75f9-DIRTY3b215dc3

//> using options -experimental -language:experimental.namedTuples

import io.github.quafadas.scautable.scautable.*
import io.github.quafadas.scautable.scautable
import io.github.quafadas.scautable.CSV
import io.github.quafadas.scautable.CSV.*
import NamedTuple.*

enum Gender :
  case Male, Female

@main def titanic =

  def csv = CSV.resource("titanic.csv")
  val i = csv.take(1)

  println(csv.headers.mkString(", "))

  def data = csv
    .mapColumn["Sex", Gender]((x: String) => Gender.valueOf(x.capitalize))
    .dropColumn["PassengerId"]
    .mapColumn["Age", Option[Double]](_.toDoubleOption)
    .mapColumn["Survived", Boolean](_ == "1")


  def survivedCol = data.column["Survived"]
  def surived= survivedCol.foldLeft((0, 0, 0.0)){case (acc, survived) =>
    val survivedI = if survived then 1 else 0
    (acc._1 + survivedI, acc._2 + 1, 100 * acc._1.toDouble / acc._2.toDouble)
  }.withNames[("Survived", "Total", "%")]

  val dataArr = data.toArray
  println(data.toArray.take(20).consoleFormatNt)
  // scautable.desktopShowNt(dataArr) // Will pop up a browser window with the data

  val sex: List[(Gender, Int)] = dataArr.map(_.Sex).groupMapReduce(identity)(_ => 1)(_ + _).toList

  val age = dataArr.map(_.Age).groupMapReduce(identity)(_ => 1)(_ + _).toList

  val group =
    dataArr
      .map(x => (x.Survived, x.Sex).withNames[("Survived", "Sex")])
      .groupMapReduce(_.Sex)(x => (if(x.Survived) 1 else 0, 1, 0.0)) {
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

  println("Surived: ")
  println(List(surived).consoleFormatNt)

  println("Gender Info")
  println(sex.consoleFormat)

  println("Survived By Gender")
  println(group.consoleFormatNt())
