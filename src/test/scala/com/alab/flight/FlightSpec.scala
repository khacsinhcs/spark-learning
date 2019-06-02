package com.alab.flight

import com.alab.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.scalatest.WordSpec

class FlightSpec extends WordSpec
  with SparkSessionTestWrapper
  with DataFrameComparer {

  trait FlightDFTest extends FlightDF {
    override def loadData(): DataFrame = spark.read.option("inferScheme", "true")
      .option("header", "true")
      .csv("data/flight-data/csv/2015-summary.csv")
  }

  object FlightRepositoryTest
    extends FlightRepository
      with FlightDFTest

  "Flight repository" should {
    "Find max row" in {
      val maxFlight = FlightRepositoryTest.maxRow()
      println(maxFlight)
    }
    "To dataset" in {
      FlightRepositoryTest.toDataset().filter(flight => flight.total == 20).show(10)
    }
  }
}
