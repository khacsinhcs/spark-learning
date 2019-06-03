package com.alab.flight

import com.alab.{DataFrameLoader, RowMaterialize}
import org.apache.spark.sql._

case class Flight(from: String, des: String, total: Int) extends Serializable

trait FlightDF extends DataFrameLoader

object FlightMaterialize {
  implicit val flightMaterial: RowMaterialize[Flight] = new RowMaterialize[Flight] {
    override def materialize(row: Row): Flight = Flight(row.getString(0), row.getString(1), row.getString(2).toInt)
  }
}

trait FlightCsvDF extends FlightDF {
  override def loadDataFrame(): DataFrame = spark.read.option("inferScheme", "true")
    .option("header", "true")
    .csv("data/flight-data/csv/*")
}

trait FlightRepository
  extends FlightDF {

  import FlightMaterialize._
  import com.alab.DataFrameSyntax._
  import com.alab.MaterializeOps._
  import org.apache.spark.sql.functions._


  implicit val encoder: Encoder[Flight] = Encoders.kryo[Flight]


  lazy val ds: Dataset[Flight] = df.toDs()
  def count: Long = df.count()

  def showRows(num: Int): Unit = df.show(num)

  def maxFight(): Option[Flight] = df.orderBy(desc("count")).take(1).map(row => row.materialize()) match {
    case Array(row: Flight) => Option(row)
    case _ => None
  }
}

object FlightRepository extends FlightRepository with FlightCsvDF


object FlightTest extends FlightCsvDF {
  def main(args: Array[String]): Unit = {

  }

}
