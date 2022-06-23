package me.rotemfo.googleads.schema

import me.rotemfo.googleads.schema.CommonSchema._
import org.apache.spark.sql.types._

//noinspection DuplicatedCode
object CampaignSchema {
  // @formatter:off
  lazy val colAdvertisingChannelType: String = "advertisingChannelType"
  lazy val colAmountMicros: String           = "amountMicros"
  lazy val colBiddingStrategyType: String    = "biddingStrategyType"
  lazy val colCampaignBudget: String         = "campaignBudget"
  lazy val colCampaignGroup: String         = "campaignGroup"
  lazy val colExperimentType: String         = "experimentType"
  lazy val colHour: String                   = "hour"
  // @formatter:on

  private lazy val campaignStruct: StructType =
    StructType(
      Seq(
        StructField(colId, StringType, nullable = true),
        StructField(colName, StringType, nullable = true),
        StructField(colStatus, StringType, nullable = true),
        StructField(colCampaignGroup, StringType, nullable = true),
        StructField(colAdvertisingChannelType, StringType, nullable = true),
        StructField(colBaseCampaign, StringType, nullable = true),
        StructField(colBiddingStrategyType, StringType, nullable = true),
        StructField(colExperimentType, StringType, nullable = true),
        //        StructField(
        //          "frequencyCaps",
        //          ArrayType(
        //            StructType(
        //              Seq(
        //                StructField(colCap, LongType, true),
        //                StructField(
        //                  "key",
        //                  StructType(
        //                    Seq(
        //                      StructField(colEventType, StringType, true),
        //                      StructField(level, StringType, true),
        //                      StructField(timeLength, LongType, true),
        //                      StructField(timeUnit, StringType, true)
        //                    )
        //                  ),
        //                  nullable = true
        //                )
        //              )
        //            )
        //          ),
        //          nullable = true
        //        ),
        StructField(
          colLabels,
          ArrayType(StringType, containsNull = true),
          nullable = true
        )
      )
    )

  private lazy val metricsStruct: StructType =
    StructType(
      Seq(
        StructField(colAllConversions, LongType, nullable = true),
        StructField(colClicks, LongType, nullable = true),
        StructField(colConversions, LongType, nullable = true),
        StructField(colCostMicros, LongType, nullable = true),
        StructField(colImpressions, LongType, nullable = true),
        StructField(colSearchImpressionShare, FloatType, nullable = true),
        StructField(colVideoViews, LongType, nullable = true),
        StructField(colViewThroughConversions, LongType, nullable = true)
      )
    )

  private lazy val campaignBudgetStruct: StructType =
    StructType(
      Seq(StructField(colAmountMicros, LongType, nullable = true))
    )

  private lazy val segmentsStruct: StructType =
    StructType(
      Seq(
        StructField(colAdNetworkType, StringType, nullable = true),
        StructField(colDate, StringType, nullable = true),
        StructField(colHour, LongType, nullable = true)
      )
    )

  val campaignSchema: StructType =
    StructType(
      Seq(
        StructField(colCampaign, campaignStruct, nullable = true),
        StructField(colMetrics, metricsStruct, nullable = true),
        StructField(colCampaignBudget, campaignBudgetStruct, nullable = true),
        StructField(colSegments, segmentsStruct, nullable = true)
      )
    )
}
