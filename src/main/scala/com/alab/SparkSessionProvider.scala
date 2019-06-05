package com.alab

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSessionProvider extends Serializable {

  implicit lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}

trait DataFrameLoader {
  def df: DataFrame

  def loadDataFrame()(implicit spark: SparkSession): DataFrame
}