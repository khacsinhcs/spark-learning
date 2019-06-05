package com.alab

import org.apache.spark.sql.SparkSession

object SparkSessionDataProviderTest {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}
