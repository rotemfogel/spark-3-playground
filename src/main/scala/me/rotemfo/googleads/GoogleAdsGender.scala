package me.rotemfo.googleads

import me.rotemfo.common.functions.camelToSnake
import me.rotemfo.googleads.schema.CommonSchema._
import me.rotemfo.googleads.schema.GenderSchema.genderViewSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}

//noinspection DuplicatedCode
object GoogleAdsGender extends BaseGoogleAdsApplication {

  /** list of field Mapping root Keys to filter by when flattening the dataframe
    *
    * @return Sequence of Strings
    */
  override protected val fieldMappingFilters: Seq[String] = Seq(colCampaign, colAdGroup)

  /** the schema from which the code extracts the dataframe
    *
    * @return StructType
    */
  override protected val schema: StructType = genderViewSchema

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
        (colClicks, LongType),
        (colCostMicros, LongType),
        (colImpressions, LongType)
      ).map((f: (String, NumericType)) => StructField(camelToSnake(f._1), f._2, nullable = true))
    )

  /** class specific transformation function
    * to perform on top of the common dataframe
    *
    * @return DataFrame
    */
  override protected def specificTransformFn(df: DataFrame)(implicit spark: SQLContext): DataFrame =
    df
}