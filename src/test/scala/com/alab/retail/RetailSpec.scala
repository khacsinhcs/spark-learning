package com.alab.retail

import com.alab.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{Matchers, WordSpec}

class RetailSpec extends WordSpec
  with Matchers
  with SparkSessionTestWrapper
  with DataFrameComparer {

  import com.alab.retail.data._

  val df: DataFrame = RetailRepository.df

  trait RetailCsvDF extends RetailDataLoader {
    override def loadDataFrame(): DataFrame = spark.read
      .option("header", "true").option("inferSchema", "true")
      .csv("data/retail-data/all/online-retail-dataset.csv")
  }

  object RetailRepository extends RetailRepository with RetailCsvDF

  "Show schema" in {
    RetailRepository.showSchema()
    df.columns should contain allOf("Country", "UnitPrice")
  }

  "material row to Retail" in {
    import com.alab.MaterializeOps._
    import com.alab.retail.RetailMaterialize._

    val row: Row = df.take(1)(0)
    val retail: Retail = retailMaterial.materialize(row)
    println(retail)

    val anotherRetail = row.materialize()

    retail should be(anotherRetail)
  }

}
