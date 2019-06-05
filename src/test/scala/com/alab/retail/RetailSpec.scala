package com.alab.retail

import com.alab.SparkSessionDataProviderTest
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{Matchers, WordSpec}

class RetailSpec extends WordSpec
  with Matchers
  with DataFrameComparer {

  import com.alab.retail.data._

  val df: DataFrame = RetailRepository.df

  trait RetailDfTest extends RetailDataLoader {
    implicit val spark: SparkSession = SparkSessionDataProviderTest.spark

    lazy val _df: DataFrame = loadDataFrame()

    override def df: DataFrame = _df
  }

  trait RetailCsvDF extends RetailDfTest {
    override def loadDataFrame()(implicit spark: SparkSession): DataFrame = spark.read
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
