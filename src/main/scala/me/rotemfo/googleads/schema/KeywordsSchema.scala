package me.rotemfo.googleads.schema

import me.rotemfo.googleads.schema.CommonSchema._
import org.apache.spark.sql.types._

//noinspection DuplicatedCode
object KeywordsSchema {
  // @formatter:off
  lazy val colAdGroupCriterion: String                  = "adGroupCriterion"
  lazy val colCriterionId: String                       = "criterionId"
  lazy val colHistoricalCreativeQualityScore: String    = "historicalCreativeQualityScore"
  lazy val colHistoricalLandingPageQualityScore: String = "historicalLandingPageQualityScore"
  lazy val colHistoricalQualityScore: String            = "historicalQualityScore"
  lazy val colHistoricalSearchPredictedCtr: String      = "historicalSearchPredictedCtr"
  lazy val colKeyword: String                           = "keyword"
  lazy val colKeywordsView: String                      = "keywordsView"
  lazy val colMatchType: String                         = "matchType"
  lazy val colSearchTopImpressionShare: String          = "searchTopImpressionShare"
  lazy val colText: String                              = "text"
  // @formatter:on

  private lazy val campaignStruct: StructType =
    StructType(
      Seq(
        StructField(colBaseCampaign, StringType, nullable = true),
        StructField(colId, StringType, nullable = true),
        StructField(
          colLabels,
          ArrayType(StringType, containsNull = true),
          nullable = true
        ),
        StructField(colName, StringType, nullable = true),
        StructField(colStatus, StringType, nullable = true)
      )
    )

  private lazy val adGroupStruct: StructType =
    StructType(
      Seq(
        StructField(colBaseAdGroup, StringType, nullable = true),
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
        StructField(colAverageCpc, FloatType, nullable = true),
        StructField(colClicks, LongType, nullable = true),
        StructField(colConversions, FloatType, nullable = true),
        StructField(colEngagements, LongType, nullable = true),
        StructField(colHistoricalCreativeQualityScore, StringType, nullable = true),
        StructField(colHistoricalLandingPageQualityScore, StringType, nullable = true),
        StructField(colHistoricalQualityScore, IntegerType, nullable = true),
        StructField(colHistoricalSearchPredictedCtr, StringType, nullable = true),
        StructField(colImpressions, LongType, nullable = true),
        StructField(colInteractions, LongType, nullable = true),
        StructField(colSearchImpressionShare, FloatType, nullable = true),
        StructField(colSearchTopImpressionShare, FloatType, nullable = true),
        StructField(colVideoViews, LongType, nullable = true)
      )
    )

  private lazy val adGroupCriterionStruct: StructType =
    StructType(
      Seq(
        StructField(colCriterionId, StringType, nullable = true)
        //        @Rotem: these fields are always returned in the schema, so no need for special treatment
        //        StructField(
        //          colKeyword,
        //          StructType(
        //            Seq(
        //              StructField(colMatchType, StringType, nullable = true),
        //              StructField(colText, StringType, nullable = true)
        //            )
        //          ),
        //          nullable = true
        //        )
      )
    )

  private lazy val segmentsStruct: StructType =
    StructType(
      Seq(
        StructField(colDate, StringType, nullable = true)
      )
    )

  private lazy val keywordsViewStruct: StructType =
    StructType(
      Seq(
        StructField(colResourceName, StringType, nullable = true)
      )
    )

  lazy val keywordsViewSchema: StructType =
    StructType(
      Seq(
        StructField(colCampaign, campaignStruct, nullable = true),
        StructField(colAdGroup, adGroupStruct, nullable = true),
        StructField(colMetrics, metricsStruct, nullable = true),
        StructField(colAdGroupCriterion, adGroupCriterionStruct, nullable = true),
        StructField(colSegments, segmentsStruct, nullable = true),
        StructField(colKeywordsView, keywordsViewStruct, nullable = true)
      )
    )
}
