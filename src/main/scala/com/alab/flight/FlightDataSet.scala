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

object Flight extends FlightCSV {
  def main(args: Array[String]): Unit = {
    Flight.loadData()
  }
}
