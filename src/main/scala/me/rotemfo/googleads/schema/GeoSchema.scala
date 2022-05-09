package me.rotemfo.googleads.schema

import me.rotemfo.googleads.schema.CommonSchema._
import org.apache.spark.sql.types._

//noinspection DuplicatedCode
object GeoSchema {
  // @formatter:off
  lazy val colCountryCriterionId: String = "countryCriterionId"
  lazy val colDevice: String             = "device"
  lazy val colGeographicView: String     = "geographicView"
  lazy val colLocationType: String       = "locationType"
  lazy val colStartDate: String          = "startDate"
  // @formatter:on

  private lazy val campaignStruct: StructType =
    StructType(
      Seq(
        StructField(colId, StringType, nullable = true),
        StructField(colName, StringType, nullable = true),
        StructField(colStartDate, StringType, nullable = true)
      )
    )

  private lazy val adGroupStruct: StructType =
    StructType(
      Seq(
        StructField(colId, StringType, nullable = true),
        StructField(colName, StringType, nullable = true)
      )
    )

  private lazy val metricsStruct: StructType =
    StructType(
      Seq(
        StructField(colAllConversions, LongType, nullable = true),
        StructField(colAverageCpc, FloatType, nullable = true),
        StructField(colClicks, LongType, nullable = true),
        StructField(colConversions, LongType, nullable = true),
        StructField(colCrossDeviceConversions, FloatType, nullable = true),
        StructField(colCtr, FloatType, nullable = true),
        StructField(colImpressions, LongType, nullable = true),
        StructField(colInteractions, LongType, nullable = true),
        StructField(colVideoViews, LongType, nullable = true),
        StructField(colViewThroughConversions, LongType, nullable = true)
      )
    )

  private lazy val segmentsStruct: StructType =
    StructType(
      Seq(
        StructField(colDate, StringType, nullable = true),
        StructField(colDevice, StringType, nullable = true)
      )
    )

  private lazy val geographicViewStruct: StructType =
    StructType(
      Seq(
        StructField(colCountryCriterionId, StringType, nullable = true),
        StructField(colLocationType, StringType, nullable = true)
      )
    )

  lazy val geographicViewSchema: StructType =
    StructType(
      Seq(
        StructField(colCampaign, campaignStruct, nullable = true),
        StructField(colAdGroup, adGroupStruct, nullable = true),
        StructField(colMetrics, metricsStruct, nullable = true),
        StructField(colSegments, segmentsStruct, nullable = true),
        StructField(colGeographicView, geographicViewStruct, nullable = true)
      )
    )
}
