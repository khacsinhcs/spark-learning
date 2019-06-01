package com.alab.flight

import com.alab.SparkSessionWrapper
import org.apache.spark.sql.DataFrame

trait FlightDataSet extends SparkSessionWrapper {
  def loadData(): DataFrame = {
    spark.read.option("inferScheme", "true")
      .option("header", "true")
      .csv("data/flight-data/csv/2015-summary.csv")
  }

}

object Flight extends FlightDataSet {
  def main(args: Array[String]): Unit = {
    Flight.loadData()
  }
}
