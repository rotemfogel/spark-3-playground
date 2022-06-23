package me.rotemfo.googleads

import me.rotemfo.common.functions.camelToSnake
import me.rotemfo.googleads.schema.CommonSchema._
import me.rotemfo.googleads.schema.KeywordsSchema._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

//noinspection DuplicatedCode
object GoogleAdsKeywords extends BaseGoogleAdsApplication {

  lazy val colSnakeMatchType: String = camelToSnake(colMatchType)

  /** list of field Mapping root Keys to filter by when flattening the dataframe
    *
    * @return Sequence of Strings
    */
  override protected val fieldMappingFilters: Seq[String] =
    Seq(colCampaign, colAdGroup, colAdGroupCriterion)

  /** the schema from which the code extracts the dataframe
    *
    * @return StructType
    */
  override protected val schema: StructType = keywordsViewSchema

  /** schema to define which columns to convert from Null to Zero
    * NULL numeric values to 0 converted to input column data type
    * must override in child class !
    *
    * @return StructType
    */
  override val nullToZeroSchema: StructType =
    StructType(
      Seq(
        (colAbsoluteTopImpressionPercentage, FloatType),
        (colAllConversions, LongType),
        (colAverageCpc, FloatType),
        (colClicks, LongType),
        (colConversions, FloatType),
        (colEngagements, LongType),
        (colHistoricalQualityScore, IntegerType),
        (colImpressions, LongType),
        (colInteractions, LongType),
        (colSearchImpressionShare, FloatType),
        (colSearchTopImpressionShare, FloatType),
        (colVideoViews, LongType)
      ).map((f: (String, NumericType)) => StructField(camelToSnake(f._1), f._2, nullable = true))
    )

  override protected def generateSelectList(
      fieldsMap: Map[String, Array[GoogleAdsKeywords.CompoundColumn]]
  ): Seq[String] = {
    super.generateSelectList(fieldsMap) ++ Seq(colText, colSnakeMatchType)
  }

  /** class specific transformation function
    * to perform on top of the common dataframe
    *
    * @return
    */
  override protected def specificTransformFn(
      df: DataFrame
  )(implicit spark: SQLContext): DataFrame = {
    val campaignLabels: String = "campaign_labels"
    val adGroupLabels: String  = "ad_group_labels"
    val newDf: DataFrame = df
      .transform(handleLabels(campaignLabels))
      .transform(handleLabels(adGroupLabels))
      .withColumn(colKeyword, df(colAdGroupCriterion).getItem(colKeyword))
    newDf
      .withColumn(colText, newDf(colKeyword).getItem(colText))
      .withColumn(colSnakeMatchType, newDf(colKeyword).getItem(colMatchType))
      .drop(colAdGroupCriterion, colKeyword)
  }
}
