package com.alab

import org.apache.spark.sql.DataFrame

trait FlightDataSet extends SparkSessionWrapper {
  def loadData(): DataFrame = {
    spark.read.option("inferScheme", "true")
      .option("header", "true")
      .load("./flight-data/csv/2015-summary.csv")
  }

}

object FlightDataSet extends FlightDataSet {
  def main(args: Array[String]): Unit = {
    FlightDataSet.loadData()
  }
}
