package com.alab.flight

import com.alab.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest.{Matchers, WordSpec}

class FlightSpec extends WordSpec
  with Matchers
  with SparkSessionTestWrapper
  with DataFrameComparer {

  trait FlightDFTest extends FlightDF {

    override def loadDataFrame(): DataFrame = spark.read.option("inferScheme", "true")
      .option("header", "true")
      .csv("data/flight-data/csv/2015-summary.csv")
  }

  object FlightRepositoryTest
    extends FlightRepository
      with FlightDFTest

  "Flight repository" should {
    "Find max row" in {
      FlightRepositoryTest.maxFight() should not be None
      FlightRepositoryTest.df.isStreaming should be(false)
      FlightRepositoryTest.df.printSchema()
    }
    "To dataset" in {
      FlightRepositoryTest.ds.filter(flight => flight.total == 20).show(10)
      FlightRepositoryTest.ds.printSchema()
    }
  }

  "Exploring api" should {
    import FlightMaterialize._
    import com.alab.DataFrameSyntax._

    val df: DataFrame = FlightTest.df

    "sort dataframe by count" in {
      df.sort("count").show(30)
    }

    "project columns and sort by count by sql" in {
      df.createOrReplaceTempView("flight_data_2015")
      val sqlWay = spark.sql(
        """
          |SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME
          |FROM flight_data_2015
          |ORDER BY count DESC
        """.stripMargin)
      sqlWay.show(20)
      sqlWay.explain()
    }

    "project columns and sort by count by code" in {
      val codeWay = df
        .select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")
        .orderBy("count")
      codeWay.show(20)
      codeWay.explain()
    }

    "sum all total flight by RDD" in {
      val rdd: RDD[Flight] = df.toRDD()
      val totalFlight = rdd.map(f => f.total).fold(0)(_ + _)
      println(totalFlight)
    }

  }
}
