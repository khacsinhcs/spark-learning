package com.alab.flight

import com.alab.SparkSessionWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class Flight(from: String, des: String, total: Int)

trait FlightDF extends SparkSessionWrapper {
  def loadData(): DataFrame
}

object FlightRDD {

  implicit class ToRDD(df: DataFrame) {
    def toRDD: RDD[Flight] = df.rdd.map(row => Flight(row.getString(0), row.getString(1), row.getString(2).toInt))
  }

}

trait FlightCsvDF extends FlightDF {
  def loadData(): DataFrame = spark.read.option("inferScheme", "true")
    .option("header", "true")
    .csv("data/flight-data/csv/*")
}

trait FlightRepository extends FlightDF {

}

object FlightRepository extends FlightRepository with FlightCsvDF {

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
