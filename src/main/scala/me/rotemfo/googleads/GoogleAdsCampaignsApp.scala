package me.rotemfo.googleads

import me.rotemfo.common.functions.camelToSnake
import me.rotemfo.googleads.schema.CampaignSchema._
import me.rotemfo.googleads.schema.CommonSchema._
import org.apache.spark.sql.AdditionalFunctions.intPadding
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//noinspection DuplicatedCode
object GoogleAdsCampaignsApp extends BaseGoogleAdsApp {

  /** list of field Mapping root Keys to filter by when flattening the dataframe
    *
    * @return Sequence of Strings
    */
  override protected val fieldMappingFilters: Seq[String] = Seq()

  /** the schema from which the code extracts the dataframe
    *
    * @return StructType
    */
  override protected val schema: StructType = campaignSchema

  /** list of columns to partition by the dataframe
    *
    * @return Sequence of Strings
    */
  override protected val partitionByColumns: Seq[String] = Seq(accountIdColumn, dateColumn, colHour)

  /** schema to define which columns to convert from Null to Zero
    * NULL numeric values to 0 converted to input column data type
    * must override in child class !
    *
    * @return StructType
    */
  override protected val nullToZeroSchema: StructType = StructType(
    Seq(
      (colAllConversions, LongType),
      (colConversions, LongType),
      (colSearchImpressionShare, FloatType)
    ).map((f: (String, NumericType)) => StructField(camelToSnake(f._1), f._2, nullable = true))
  )

  /** class specific transformation function
    * to perform on top of the common dataframe
    *
    * @return
    */
  override protected val specificTransformFn: DataFrame => DataFrame = (df: DataFrame) =>
    df.transform(handleLabels)
      .withColumn(colHour, intPadding(col(colHour)).cast(StringType))
}
