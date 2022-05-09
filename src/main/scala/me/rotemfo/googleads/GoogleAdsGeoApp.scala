package me.rotemfo.googleads

import me.rotemfo.common.functions.camelToSnake
import me.rotemfo.googleads.schema.CommonSchema._
import me.rotemfo.googleads.schema.GeoSchema._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types._

//noinspection DuplicatedCode
object GoogleAdsGeoApp extends BaseGoogleAdsApp {

  /** list of field Mapping root Keys to filter by when flattening the dataframe
    *
    * @return Sequence of Strings
    */
  override protected val fieldMappingFilters: Seq[String] = Seq(colCampaign, colAdGroup)

  /** the schema from which the code extracts the dataframe
    *
    * @return StructType
    */
  override protected val schema: StructType = geographicViewSchema

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
        (colCrossDeviceConversions, FloatType),
        (colCtr, FloatType),
        (colImpressions, LongType),
        (colInteractions, LongType),
        (colVideoViews, LongType),
        (colViewThroughConversions, LongType)
      ).map((f: (String, NumericType)) => StructField(camelToSnake(f._1), f._2, nullable = true))
    )

  /** class specific transformation function
    * to perform on top of the common dataframe
    *
    * @return
    */
  override protected val specificTransformFn: DataFrame => DataFrame = (df: DataFrame) => {
    val colCampaignStartDate: String = "campaign_start_date"
    df.withColumn(colCampaignStartDate, to_date(col(colCampaignStartDate), "yyyy-MM-dd"))
  }
}
