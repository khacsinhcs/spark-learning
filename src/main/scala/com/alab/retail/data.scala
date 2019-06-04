package com.alab.retail

import com.alab.DataFrameLoader
import org.apache.spark.sql.DataFrame

package object data {

  trait RetailDataLoader extends DataFrameLoader

  trait RetailCsvData extends RetailDataLoader {
    override def loadDataFrame(): DataFrame = spark.read.format("csv")
      .option("header", "true").option("inferSchema", "true")
      .load("/data/retail-data/by-day/*.csv")
  }

}
