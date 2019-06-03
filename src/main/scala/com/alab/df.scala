package com.alab

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

import scala.reflect.ClassTag

object df {

  object RDDOps {

    implicit class ToRDDSyntax(dataFrame: DataFrame) {
      def toRDD[T]()(implicit rowMaterialize: RowMaterialize[T], classTag: ClassTag[T]): RDD[T] = dataFrame.rdd.map(row => rowMaterialize.materialize(row))
    }

  }

  object DataSetOps {

    implicit class ToDsOps(dataFrame: DataFrame) {
      def toDs[T]()(implicit rowMaterialize: RowMaterialize[T], encoder: Encoder[T], classTag: ClassTag[T]): Dataset[T] = dataFrame.map(row => rowMaterialize.materialize(row))
    }

  }

}
