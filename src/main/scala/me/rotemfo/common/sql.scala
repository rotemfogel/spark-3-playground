package me.rotemfo.common

import me.rotemfo.common.functions.udfEmptyStringToNull
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object sql {
  implicit class DataFrameExSql(dataFrame: DataFrame) {
    def repairStringCol(colNames: String*): DataFrame = {
      colNames.foldLeft(dataFrame) { case (df, colName) =>
        df.withColumn(colName, udfEmptyStringToNull(col(colName)))
      }
    }
    def repairColumns: DataFrame = {
      val columns: Array[String] =
        dataFrame.schema.fields.filter(_.dataType == StringType).map(_.name)
      dataFrame.repairStringCol(columns: _*)
    }
  }
}
