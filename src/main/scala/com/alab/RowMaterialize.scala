package com.alab

import org.apache.spark.sql.Row

trait RowMaterialize[T] {
  def materialize(row: Row): T
}

object MaterializeOps {

  implicit class MaterializeSyntax(row: Row) {
    def materialize[T]()(implicit rowMaterialize: RowMaterialize[T]): T = rowMaterialize.materialize(row)
  }

}