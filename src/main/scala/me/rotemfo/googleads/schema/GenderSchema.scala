package me.rotemfo.googleads.schema

import me.rotemfo.googleads.schema.CommonSchema._
import org.apache.spark.sql.types._

//noinspection DuplicatedCode
object GenderSchema {
  lazy val colGenderView: String = "genderView"

  private lazy val campaignStruct: StructType =
    StructType(
      Seq(
        StructField(colId, StringType, nullable = true),
        StructField(colName, StringType, nullable = true)
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
        StructField(colClicks, LongType, nullable = true),
        StructField(colCostMicros, LongType, nullable = true),
        StructField(colImpressions, LongType, nullable = true)
      )
    )

  private lazy val segmentsStruct: StructType =
    StructType(
      Seq(
        StructField(colDate, StringType, nullable = true)
      )
    )

  private lazy val genderViewStruct: StructType =
    StructType(
      Seq(
        StructField(colResourceName, StringType, nullable = true)
      )
    )

  lazy val genderViewSchema: StructType =
    StructType(
      Seq(
        StructField(colCampaign, campaignStruct, nullable = true),
        StructField(colAdGroup, adGroupStruct, nullable = true),
        StructField(colMetrics, metricsStruct, nullable = true),
        StructField(colSegments, segmentsStruct, nullable = true),
        StructField(colGenderView, genderViewStruct, nullable = true)
      )
    )
}