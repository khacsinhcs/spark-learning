package com.alab.flight

import com.alab.{DataFrameLoader, RowMaterialize}
import org.apache.spark.sql._

case class Flight(from: String, des: String, total: Int)

trait FlightDF extends DataFrameLoader

object RowToFight {
  implicit val flightMaterial: RowMaterialize[Flight] = new RowMaterialize[Flight] {
    override def materialize(row: Row): Flight = Flight(row.getString(0), row.getString(1), row.getString(2).toInt)
  }
}

trait FlightCsvDF extends FlightDF {
  override def loadDataFrame(): DataFrame = spark.read.option("inferScheme", "true")
    .option("header", "true")
    .csv("data/flight-data/csv/*")
}

trait FlightRepository
  extends FlightDF {

  import RowToFight._
  import com.alab.DataFrameSyntax._
  import com.alab.MaterializeOps._
  import org.apache.spark.sql.functions._


  implicit val encoder: Encoder[Flight] = Encoders.kryo[Flight]


  lazy val ds: Dataset[Flight] = df.toDs()
  def count: Long = df.count()

  def showRows(num: Int): Unit = df.show(num)

  def maxFight(): Option[Flight] = df.orderBy(desc("count")).take(1).map(row => row.materialize()) match {
    case Array(row: Flight) => Option(row)
    case _ => None
  }
}

object FlightRepository extends FlightRepository with FlightCsvDF


object FlightTest extends FlightCsvDF {
  def main(args: Array[String]): Unit = {
    val df: DataFrame = FlightTest.loadDataFrame()
    df.sort("count").show(30)

    df.createOrReplaceTempView("flight_data_2015")
    val sqlWay = spark.sql(
      """
        |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME
        |FROM flight_data_2015
        |ORDER BY count DESC
      """.stripMargin)
    sqlWay.show(20)
    sqlWay.explain()

    df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
      .orderBy("count")
      .show(30)

    import RowToFight._
    import com.alab.DataFrameSyntax._
    val totalFlight = df.toRDD.map(f => f.total).fold(0)((f1, f2) => f1 + f2)
    println(totalFlight)
  }

}
