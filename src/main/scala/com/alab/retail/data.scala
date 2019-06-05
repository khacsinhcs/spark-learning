package com.alab.retail

import com.alab.{DataFrameLoader, SparkSessionProvider}
import org.apache.spark.sql.{DataFrame, SparkSession}

package object data {

  trait RetailDataLoader extends DataFrameLoader

  trait ProductRetailDataLoader extends RetailDataLoader {
    implicit val spark: SparkSession = SparkSessionProvider.spark

    lazy val _df = loadDataFrame()

    override def df: DataFrame = _df
  }

  trait RetailCsvData extends ProductRetailDataLoader {

    override def loadDataFrame()(implicit spark: SparkSession): DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load("data/retail-data/by-day/*.csv")
  }

}
