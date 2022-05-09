package me.rotemfo.stam

import me.rotemfo.common.functions.fromJson

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source

object PartsApp {
  // private def _split(s: String): String = s.split("=").last

  private val format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

  def main(args: Array[String]): Unit = {
    val source = Source.fromFile(new File("./data/1.json"))
    val json = source.getLines().toSeq.mkString("")
    val map: Map[String, Map[LocalDate, Seq[Int]]] =
      fromJson[Map[String, Map[String, Seq[Int]]]](json)
        .mapValues(_.map({ case (k, v) => (LocalDate.parse(k, format), v) }).toMap)
    map.foreach({ case (k, v) =>
      val keys = v.keys.toSeq.sorted
      for (i <- 0 to keys.length - 1) {
        if (i > 0)
          if (!keys(i).minusDays(1).equals(keys(i - 1)))
            println(s"$k: ${keys(i - 1)}")
      }
    })
    source.close()
  }
}
