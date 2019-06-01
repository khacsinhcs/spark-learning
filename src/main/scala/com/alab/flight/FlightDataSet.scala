package com.alab.flight

import com.alab.SparkSessionWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

case class Flight(from: String, des: String, total: Int)

trait FlightDS extends SparkSessionWrapper {
  def loadData(): DataFrame
}

object FlightRDD {

  implicit class ToRDD(df: DataFrame) {
    def toRDD: RDD[Flight] = df.rdd.map(row => Flight(row.getString(0), row.getString(1), row.getString(2).toInt))
  }

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
    df.sort("count").show(30)

    import FlightRDD._
    val totalFlight = df.toRDD.map(f => f.total).fold(0)((f1, f2) => f1 + f2)
    println(totalFlight)
  }

}
