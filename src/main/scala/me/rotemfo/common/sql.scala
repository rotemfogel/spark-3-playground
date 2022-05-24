package me.rotemfo.common

import me.rotemfo.common.Schema.{colNamePostsUrlParams, urlParamsKeys}
import me.rotemfo.common.functions.{getJsonObjectNullSafe, udfEmptyStringToNull, udfUrlParamsParser}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

object sql {
  implicit class DataFrameExSql(dataFrame: DataFrame) {
    def repairStringCol(colNamePostsUrlParamss: String*): DataFrame = {
      colNamePostsUrlParamss.foldLeft(dataFrame) { case (df, colNamePostsUrlParams) =>
        df.withColumn(colNamePostsUrlParams, udfEmptyStringToNull(col(colNamePostsUrlParams)))
      }
    }

    def repairColumns: DataFrame = {
      val columns: Array[String] =
        dataFrame.schema.fields.filter(_.dataType == StringType).map(_.name)
      dataFrame.repairStringCol(columns: _*)
    }

    @transient private lazy val colNamePostsUrlParamsPostsUrlParamsTrafficSourceParam =
      "traffic_source_param"

    /** get a key from source JSON column
      *
      * @param df               the dataframe
      * @param sourceColumnName the source column name
      * @param columnDef        the column definition
      * @return DataFrame
      */
    def getKey(df: DataFrame, sourceColumnName: String, columnDef: StructField): DataFrame = {
      val specialKeys: Map[String, String] = Map(
        colNamePostsUrlParamsPostsUrlParamsTrafficSourceParam -> "source"
      )

      df.withColumn(
        columnDef.name,
        getJsonObjectNullSafe(
          col(sourceColumnName),
          s"$$.${specialKeys.getOrElse(columnDef.name, columnDef.name)}"
        ).cast(columnDef.dataType)
      )
    }

    /** add columns based on `keys`
      *
      * @param dataFrame        the DataFrame
      * @param sourceColumnName the source column name
      * @param keys             key definitions
      * @return
      */
    def getKeys(dataFrame: DataFrame, sourceColumnName: String, keys: StructType): DataFrame = {
      keys.foldLeft(dataFrame) { (df, key) =>
        getKey(df, sourceColumnName, key)
      }
    }

    /** convert url_params to
      *
      * @return
      */
    def convertUrlParams(urlKeys: StructType = urlParamsKeys): DataFrame = {
      getKeys(
        dataFrame.withColumn(colNamePostsUrlParams, udfUrlParamsParser(col(colNamePostsUrlParams))),
        colNamePostsUrlParams,
        urlKeys
      )
    }

    /** cache dataframe and perform a transformation
      *
      * @param f the function on the DataFrame
      * @tparam A A
      * @return A
      */
    def withCached[A](f: DataFrame => A): A = withCached(StorageLevel.MEMORY_AND_DISK)(f)

    /** cache dataframe and perform a transformation
      *
      * @param storageLevel the storage level to cache with
      * @param f            the function on the DataFrame
      * @tparam A A
      * @return A
      */
    def withCached[A](storageLevel: StorageLevel = MEMORY_AND_DISK)(f: DataFrame => A): A = {
      dataFrame.persist(storageLevel)
      val result = f(dataFrame)
      dataFrame.unpersist
      result
    }
  }

  /** get url_first_level
    *
    * @param urlColumn the url column
    * @return Column
    */
  def getUrlFirstLevel(urlColumn: Column): Column = getUrlFirstLevel(urlColumn, lit("any"))

  /** get url_first_level
    *
    * @param urlColumn        the url column
    * @param clientTypeColumn the client_type column
    * @return
    */
  def getUrlFirstLevel(urlColumn: Column, clientTypeColumn: Column): Column = {
    val colon: String  = ":"
    val splitUrlColumn = split(urlColumn, "/")
    when(
      udfEmptyStringToNull(splitUrlColumn.getItem(0)).isNull,
      udfEmptyStringToNull(
        when(splitUrlColumn.getItem(1).eqNullSafe("account"), splitUrlColumn.getItem(2))
          // https://seekingalpha.atlassian.net/browse/DS3-1443
          .when(
            splitUrlColumn.getItem(1).eqNullSafe("marketplace") &&
              split(splitUrlColumn.getItem(2), "-").getItem(0).cast(IntegerType).isNotNull &&
              splitUrlColumn.getItem(3).isNotNull,
            concat(splitUrlColumn.getItem(1), lit(colon), splitUrlColumn.getItem(3))
          )
          // https://seekingalpha.atlassian.net/browse/DS3-1485
          .when(
            splitUrlColumn.getItem(1).eqNullSafe("alpha-picks") &&
              splitUrlColumn.getItem(2).isNotNull,
            concat(splitUrlColumn.getItem(1), lit(colon), splitUrlColumn.getItem(2))
          )
          // fix for bug in reporting https://seekingalpha.atlassian.net/browse/DS3-1318
          .when(
            splitUrlColumn.getItem(0).eqNullSafe("") &&
              splitUrlColumn.getItem(1).eqNullSafe("https:"),
            splitUrlColumn.getItem(4)
          )
          .when(urlColumn.eqNullSafe("/"), "home")
          .when(urlColumn.isNull && clientTypeColumn.startsWith("Mobile Apps"), "home")
          .otherwise(splitUrlColumn.getItem(1))
      )
    )
      .otherwise(null)
  }
}
