package com.alab.flight

import com.alab.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, WordSpec}

class FlightSpec extends WordSpec
  with Matchers
  with SparkSessionTestWrapper
  with DataFrameComparer {

  import FlightMaterialize._
  import com.alab.DataFrameSyntax._

  "Fight with csv data source" should {
    trait FlightCsvDF extends FlightDF {
      override def loadDataFrame(): DataFrame = spark.read.option("inferScheme", "true")
        .option("header", "true")
        .csv("data/flight-data/csv/2015-summary.csv")
    }

    object FlightRepositoryTest extends FlightRepository with FlightCsvDF

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

      "to RDD[Flight]" in {
        val rdd: RDD[Flight] = FlightRepositoryTest.df.toRDD()
        rdd.count() should be(FlightRepositoryTest.df.count())
      }
    }

    "Exploring api" should {

      val df: DataFrame = FlightRepositoryTest.df

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

  "Fight with json data source" should {
    trait FlightJsonDF extends FlightDF {
      override def loadDataFrame(): DataFrame = spark.read.format("json").load("data/flight-data/json/2015-summary.json")
    }

    object FlightRepositoryTest extends FlightRepository with FlightJsonDF

    val df = FlightRepositoryTest.df

    "Json DataFrame should have schema" in {
      df.printSchema()
      val schema = df.schema
      schema.head.name should be("DEST_COUNTRY_NAME")
      val columnsName = schema.map(f => f.name)
      columnsName should contain allOf("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME", "count")
    }

    "exploring expression" in {
      val select1 = df.select(expr("DEST_COUNTRY_NAME"), expr("ORIGIN_COUNTRY_NAME"))
      val select2 = df.select(col("DEST_COUNTRY_NAME"), col("ORIGIN_COUNTRY_NAME"))
      val select3 = df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME")

      assertSmallDataFrameEquality(select1, select2)
      assertSmallDataFrameEquality(select2, select3)

      df.selectExpr("DEST_COUNTRY_NAME as dest", "ORIGIN_COUNTRY_NAME as origin").printSchema()
    }

    "select expression" in {
      val select1 = df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))")
      val select2 = df.select(avg("count"), countDistinct("DEST_COUNTRY_NAME"))
      assertSmallDataFrameEquality(select1, select2)
    }

    "add column api" in {
      val select = df.withColumn("withinCountry", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME"))
      select.columns should contain("withinCountry")
      select.printSchema()
    }
  }
}
