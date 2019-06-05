package com.alab.retail

import java.util.Date

import com.alab.RowMaterialize
import com.alab.retail.data._
import org.apache.spark.sql.{Encoder, Encoders, Row}

case class Retail(invoiceNo: String,
                  stockCode: String,
                  description: String,
                  quantity: Int,
                  invoiceDate: Date,
                  unitPrice: Double,
                  customerId: Int,
                  country: String)

object RetailMaterialize {
  implicit val retailMaterial: RowMaterialize[Retail] = new RowMaterialize[Retail] {
    override def materialize(row: Row): Retail = Retail(
      row.getString(0),
      row.getString(1),
      row.getString(2),
      row.getInt(3),
      new Date(row.getString(4)),
      row.getDouble(5),
      row.getInt(6),
      row.getString(7)
    )
  }
}

trait RetailRepository extends RetailDataLoader {
  implicit val encoder: Encoder[Retail] = Encoders.kryo[Retail]

  def showSchema(): Unit = df.printSchema()
}
