package me.rotemfo.googleads.schema

object CommonSchema {
  // @formatter:off
  lazy val colAbsoluteTopImpressionPercentage: String = "absoluteTopImpressionPercentage"
  lazy val colAdGroup: String                         = "adGroup"
  lazy val colAdNetworkType: String                   = "adNetworkType"
  lazy val colAllConversions: String                  = "allConversions"
  lazy val colAverageCpc: String                      = "averageCpc"
  lazy val colBaseAdGroup: String                     = "baseAdGroup"
  lazy val colBaseCampaign: String                    = "baseCampaign"
  lazy val colCampaign: String                        = "campaign"
  lazy val colClicks: String                          = "clicks"
  lazy val colConversions: String                     = "conversions"
  lazy val colCostMicros: String                      = "costMicros"
  lazy val colCrossDeviceConversions: String          = "crossDeviceConversions"
  lazy val colCtr: String                             = "ctr"
  lazy val colCustomer: String                        = "customer"
  lazy val colDate: String                            = "date"
  lazy val colDevice: String                          = "device"
  lazy val colEngagements: String                     = "engagements"
  lazy val colId: String                              = "id"
  lazy val colImpressions: String                     = "impressions"
  lazy val colInteractions: String                    = "interactions"
  lazy val colLabels: String                          = "labels"
  lazy val colMetrics: String                         = "metrics"
  lazy val colName: String                            = "name"
  lazy val colResourceName: String                    = "resourceName"
  lazy val colSearchImpressionShare: String           = "searchImpressionShare"
  lazy val colSegments: String                        = "segments"
  lazy val colStatus: String                          = "status"
  lazy val colTargetCpaMicros: String                 = "targetCpaMicros"
  lazy val colVideoViews: String                      = "videoViews"
  lazy val colViewThroughConversions: String          = "viewThroughConversions"

  // non GoogleAds schema variables
  lazy val dateColumn: String      = "date_"
  lazy val accountIdColumn: String = "account_id"
  // @formatter:on
}
