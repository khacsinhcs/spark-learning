package com.alab.flight

import com.alab.SparkSessionWrapper
import org.apache.spark.sql.DataFrame

trait FlightDS extends SparkSessionWrapper {
  def loadData(): DataFrame
}

trait FlightCSV extends FlightDS {
  def loadData(): DataFrame = {
    spark.read.option("inferScheme", "true")
      .option("header", "true")
      .csv("data/flight-data/csv/2015-summary.csv")
  }
}

object FlightTest extends FlightCSV {
  def main(args: Array[String]): Unit = {
    val df: DataFrame = FlightTest.loadData()
    df.rdd.map(row => Flight(row.getAs[String](0), row.getAs[String](1), row.getString(2).toInt)).take(3).foreach(println(_))
  }

  case class Flight(from: String, des: String, total: Int)
}
