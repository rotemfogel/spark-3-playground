package me.rotemfo.googleads

import me.rotemfo.common.Schema.{
  colNamePostsUrl,
  colNamePostsUrlParams,
  colNameUrlFirstLevel,
  urlParamsKeys
}
import me.rotemfo.common.functions.{camelToSnake, getJsonSchema}
import me.rotemfo.common.sql.{DataFrameExSql, getUrlFirstLevel}
import me.rotemfo.googleads.schema.CommonSchema._
import me.rotemfo.googleads.schema.LandingPagesSchema._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

//noinspection DuplicatedCode
object GoogleAdsLandingPages extends BaseGoogleAdsApplication {

  lazy val urlKeys: StructType = StructType(urlParamsKeys.fields.slice(2, 8))

  /** list of field Mapping root Keys to filter by when flattening the dataframe
    *
    * @return Sequence of Strings
    */
  override protected val fieldMappingFilters: Seq[String] =
    Seq(colCustomer, colCampaign, colAdGroup)

  /** the schema from which the code extracts the dataframe
    *
    * @return StructType
    */
  override protected val schema: StructType = landingPagesSchema

  /** schema to define which columns to convert from Null to Zero
    * NULL numeric values to 0 converted to input column data type
    * must override in child class !
    *
    * @return StructType
    */
  override val nullToZeroSchema: StructType =
    StructType(
      Seq(
        (colAllConversions, LongType),
        (colAverageCpc, FloatType),
        (colClicks, LongType),
        (colConversions, LongType),
        (colEngagements, LongType),
        (colImpressions, LongType),
        (colVideoViews, LongType)
      ).map((f: (String, NumericType)) => StructField(camelToSnake(f._1), f._2, nullable = true))
    )

  /** generate a select list based on the field mapping
    *
    * @param fieldsMap the field mapping map
    * @return List of Strings
    */
  override protected def generateSelectList(
      fieldsMap: Map[String, Array[GoogleAdsLandingPages.CompoundColumn]]
  ): Seq[String] = {
    super.generateSelectList(fieldsMap) ++ urlKeys.fieldNames :+ colNameUrlFirstLevel
  }

  /** class specific transformation function
    * to perform on top of the common dataframe
    *
    * @return
    */
  override protected def specificTransformFn(
      df: DataFrame
  )(implicit spark: SQLContext): DataFrame = {
    val snakeExpandedFinalUrl: Column = col(camelToSnake(colExpandedFinalUrl))
    df.transform(handleLabels(adGroupLabels))
      // create url_params column
      .withColumn(
        colNamePostsUrlParams,
        split(snakeExpandedFinalUrl, "\\?").getItem(1)
      )
      // create url (same as in page_events)
      .withColumn(colNamePostsUrl, regexp_extract(snakeExpandedFinalUrl, ".*\\.com(.*)$", 1))
      .withColumn(colNamePostsUrl, split(col(colNamePostsUrl), "\\?").getItem(0))
      // create url_first_level
      .withColumn(colNameUrlFirstLevel, getUrlFirstLevel(col(colNamePostsUrl)))
      .drop(colNamePostsUrl)
      // convert url_params to JSON
      .convertUrlParams(urlKeys)
      .withCached { urlDf =>
        urlDf
          .withColumn(
            colNamePostsUrlParams,
            to_json(
              from_json(
                col(colNamePostsUrlParams),
                getJsonSchema(urlDf, col(colNamePostsUrlParams), urlKeys.fieldNames)
              )
            )
          )
      }
  }
}
