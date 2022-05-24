package me.rotemfo.googleads.schema

import me.rotemfo.googleads.schema.CommonSchema._
import org.apache.spark.sql.types._

//noinspection DuplicatedCode
object LandingPagesSchema {
  // @formatter:off
  lazy val colExpandedFinalUrl: String        = "expandedFinalUrl"
  lazy val colExpandedLandingPageView: String = "expandedLandingPageView"
  lazy val colLandingPageView: String         = "landingPageView"
  lazy val colServingStatus: String           = "servingStatus"
  lazy val colTrackingUrlTemplate: String     = "trackingUrlTemplate"
  lazy val colUnexpandedFinalUrl: String      = "unexpandedFinalUrl"
  // @formatter:on

  private lazy val customerStruct: StructType =
    StructType(
      Seq(
        StructField(colId, StringType, nullable = true)
      )
    )
  private lazy val campaignStruct: StructType =
    StructType(
      Seq(
        StructField(colBaseCampaign, StringType, nullable = true),
        StructField(colId, StringType, nullable = true),
        StructField(colName, StringType, nullable = true),
        StructField(colServingStatus, StringType, nullable = true),
        StructField(colTrackingUrlTemplate, StringType, nullable = true)
      )
    )

  private lazy val adGroupStruct: StructType =
    StructType(
      Seq(
        StructField(colBaseAdGroup, StringType, nullable = true),
        StructField(colCampaign, StringType, nullable = true),
        StructField(colId, StringType, nullable = true),
        StructField(
          colLabels,
          ArrayType(StringType, containsNull = true),
          nullable = true
        ),
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
        StructField(colEngagements, LongType, nullable = true),
        StructField(colImpressions, LongType, nullable = true),
        StructField(colVideoViews, LongType, nullable = true)
      )
    )

  private lazy val segmentsStruct: StructType = StructType(
    Seq(
      StructField(colAdNetworkType, StringType, nullable = true),
      StructField(colDate, StringType, nullable = true),
      StructField(colDevice, StringType, nullable = true)
    )
  )

  private val landingPageViewStruct: StructType = StructType(
    Seq(
      StructField(colUnexpandedFinalUrl, StringType, nullable = true),
      StructField(colResourceName, StringType, nullable = true)
    )
  )

  private val expandedLandingPageViewStruct: StructType = StructType(
    Seq(
      StructField(colExpandedFinalUrl, StringType, nullable = true)
    )
  )

  lazy val landingPagesSchema: StructType =
    StructType(
      Seq(
        StructField(colCustomer, customerStruct, nullable = true),
        StructField(colCampaign, campaignStruct, nullable = true),
        StructField(colAdGroup, adGroupStruct, nullable = true),
        StructField(colMetrics, metricsStruct, nullable = true),
        StructField(colSegments, segmentsStruct, nullable = true),
        StructField(colLandingPageView, landingPageViewStruct, nullable = true),
        StructField(colExpandedLandingPageView, expandedLandingPageViewStruct, nullable = true)
      )
    )
}
