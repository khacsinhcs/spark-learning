package com.alab

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}

trait DataFrameLoader extends SparkSessionWrapper {
  lazy val df: DataFrame = loadDataFrame()

  def loadDataFrame(): DataFrame
}