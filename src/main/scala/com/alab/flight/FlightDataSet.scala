package com.alab.flight

import com.alab.SparkSessionWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

case class Flight(from: String, des: String, total: Int)

object RowToFlight {

  implicit class ToFlight(row: Row) {
    def toFlight: Flight = Flight(row.getString(0), row.getString(1), row.getString(2).toInt)
  }

}

trait FlightDF extends SparkSessionWrapper {
  def loadData(): DataFrame
}

object FlightRDD {

  implicit class ToRDD(df: DataFrame) {

    import RowToFlight._

    def toRDD: RDD[Flight] = df.rdd.map(row => row.toFlight)
  }

}

trait FlightCsvDF extends FlightDF {
  def loadData(): DataFrame = spark.read.option("inferScheme", "true")
    .option("header", "true")
    .csv("data/flight-data/csv/*")
}

trait FlightRepository
  extends FlightDF {

  import RowToFlight._
  import org.apache.spark.sql.functions._

  implicit val encoder: Encoder[Flight] = Encoders.kryo[Flight]

  lazy val df: DataFrame = loadData()

  lazy val ds: Dataset[Flight] = df.map(row => row.toFlight)

  def count: Long = df.count()

  def showRows(num: Int): Unit = df.show(num)

  def maxRow(): Option[Flight] = df.orderBy(desc("count")).take(1).map(row => row.toFlight) match {
    case Array(row: Flight) => Option(row)
    case _ => None
  }

  def toDataset(): Dataset[Flight] = ds
}

object FlightRepository
  extends FlightRepository
    with FlightCsvDF {

}


object FlightTest extends FlightCsvDF {
  def main(args: Array[String]): Unit = {
    val df: DataFrame = FlightTest.loadData()
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
    import org.apache.spark.sql.functions._
    df.select(max("count")).show(1)
    import FlightRDD._
    val totalFlight = df.toRDD.map(f => f.total).fold(0)((f1, f2) => f1 + f2)
    println(totalFlight)
  }

}
