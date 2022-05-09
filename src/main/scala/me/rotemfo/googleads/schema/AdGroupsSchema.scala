package me.rotemfo.googleads.schema

import me.rotemfo.googleads.schema.CommonSchema._
import org.apache.spark.sql.types._

//noinspection DuplicatedCode
object AdGroupsSchema {
  // @formatter:off
  lazy val colBounceRate: String                      = "bounceRate"
  lazy val colCpcBidMicros: String                    = "cpcBidMicros"
  lazy val colPercentNewVisitors: String              = "percentNewVisitors"
  lazy val colTopImpressionPercentage: String         = "topImpressionPercentage"
  // @formatter:on

  private lazy val campaignStruct: StructType =
    StructType(
      Seq(
        StructField(colBaseCampaign, StringType, nullable = true),
        StructField(colId, StringType, nullable = true),
        StructField(colStatus, StringType, nullable = true)
      )
    )

  private lazy val adGroupStruct: StructType =
    StructType(
      Seq(
        StructField(colBaseAdGroup, StringType, nullable = true),
        StructField(colCampaign, StringType, nullable = true),
        StructField(colCpcBidMicros, LongType, nullable = true),
        StructField(colId, StringType, nullable = true),
        StructField(
          colLabels,
          ArrayType(StringType, containsNull = true),
          nullable = true
        ),
        StructField(colName, StringType, nullable = true),
        StructField(colStatus, StringType, nullable = true),
        StructField(colTargetCpaMicros, LongType, nullable = true)
      )
    )

  private lazy val metricsStruct: StructType =
    StructType(
      Seq(
        StructField(colAbsoluteTopImpressionPercentage, FloatType, nullable = true),
        StructField(colAllConversions, LongType, nullable = true),
        StructField(colBounceRate, FloatType, nullable = true),
        StructField(colClicks, LongType, nullable = true),
        StructField(colConversions, LongType, nullable = true),
        StructField(colCostMicros, LongType, nullable = true),
        StructField(colCrossDeviceConversions, FloatType, nullable = true),
        StructField(colImpressions, LongType, nullable = true),
        StructField(colPercentNewVisitors, FloatType, nullable = true),
        StructField(colSearchImpressionShare, FloatType, nullable = true),
        StructField(colTopImpressionPercentage, FloatType, nullable = true),
        StructField(colVideoViews, LongType, nullable = true)
      )
    )

  private lazy val segmentsStruct: StructType = StructType(
    Seq(
      StructField(colDate, StringType, nullable = true)
    )
  )

  val adGroupsSchema: StructType =
    StructType(
      Seq(
        StructField(colCampaign, campaignStruct, nullable = true),
        StructField(colAdGroup, adGroupStruct, nullable = true),
        StructField(colMetrics, metricsStruct, nullable = true),
        StructField(colSegments, segmentsStruct, nullable = true)
      )
    )
}
