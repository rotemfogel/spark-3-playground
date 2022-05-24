package me.rotemfo.common

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Schema {

  @transient lazy val colNameUrlFirstLevel                    = "url_first_level"
  @transient lazy val colNameClientType                       = "client_type"
  @transient lazy val colNamePostsSource                      = "source"
  @transient lazy val colNamePostsUrl                         = "url"
  @transient lazy val colNamePostsUrlParams                   = "url_params"
  @transient lazy val colNameUrlParamsRow                     = "url_params_row"
  @transient lazy val colNamePostsUrlParamsTrafficSourceParam = "traffic_source_param"
  @transient lazy val colNamePostsUrlParamsTrafficSourceGroup = "traffic_source_group"
  @transient lazy val colNamePostsUrlParamsTrafficUtmSource   = "utm_source"
  @transient lazy val colNamePostsUrlParamsTrafficUtmMedium   = "utm_medium"
  @transient lazy val colNamePostsUrlParamsTrafficUtmCampaign = "utm_campaign"
  @transient lazy val colNamePostsUrlParamsTrafficUtmTerm     = "utm_term"
  @transient lazy val colNamePostsUrlParamsTrafficUtmContent  = "utm_content"

  lazy val urlParamsKeys: StructType = StructType(
    Seq(
      StructField(colNameUrlParamsRow, StringType, nullable = true),
      // This is the key for `traffic_source_param`
      StructField(colNamePostsSource, StringType, nullable = true),
      StructField(colNamePostsUrlParamsTrafficSourceParam, StringType, nullable = true),
      StructField(colNamePostsUrlParamsTrafficUtmSource, StringType, nullable = true),
      StructField(colNamePostsUrlParamsTrafficUtmMedium, StringType, nullable = true),
      StructField(colNamePostsUrlParamsTrafficUtmCampaign, StringType, nullable = true),
      StructField(colNamePostsUrlParamsTrafficUtmTerm, StringType, nullable = true),
      StructField(colNamePostsUrlParamsTrafficUtmContent, StringType, nullable = true)
    )
  )
}
