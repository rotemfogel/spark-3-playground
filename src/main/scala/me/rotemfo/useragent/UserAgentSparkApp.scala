package me.rotemfo.useragent

import me.rotemfo.common.UserAgentUtils.userAgentFactory
import me.rotemfo.common.functions.{getJsonObjectNullSafe, userAgentParserBroadcast}
import me.rotemfo.common.{ParquetReaderApplication, ReaderConfig}
import nl.basjes.parse.useragent.UserAgent._
import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

object UserAgentSparkApp extends ParquetReaderApplication {

  private lazy val schema: StructType =
    StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("user_agent", StringType, nullable = false)
      )
    )

  private lazy val id: String              = "id"
  private lazy val userAgent: String       = "user_agent"
  private lazy val userAgentStruct: String = s"${userAgent}_struct"

  private val userAgentMap: Map[String, String] = Map(
    AGENT_CLASS                    -> "agent_class",
    AGENT_NAME                     -> "agent_name",
    AGENT_NAME_VERSION             -> "agent_name_version",
    AGENT_NAME_VERSION_MAJOR       -> "agent_name_version_major",
    AGENT_VERSION                  -> "agent_version",
    AGENT_VERSION_MAJOR            -> "agent_version_major",
    DEVICE_BRAND                   -> "device_brand",
    DEVICE_CLASS                   -> "device_class",
    DEVICE_NAME                    -> "device_name",
    DEVICE_VERSION                 -> "device_version",
    OPERATING_SYSTEM_CLASS         -> "operating_system_class",
    OPERATING_SYSTEM_NAME          -> "operating_system_name",
    OPERATING_SYSTEM_VERSION       -> "operating_system_version",
    OPERATING_SYSTEM_VERSION_MAJOR -> "operating_system_version_major"
  )

  private def userAgentExtractColumns(
      col: Column,
      conf: Map[String, String]
  ): DataFrame => DataFrame = df =>
    if (conf.isEmpty) df
    else {
      conf.foldLeft(df) { case (d, tuple) =>
        d.withColumn(tuple._2, getJsonObjectNullSafe(col, s"$$.${tuple._1}"))
      }
    }

  override protected def invoke(implicit p: ReaderConfig, spark: SQLContext): Unit = {
    val theUserAgent: UserAgentAnalyzer = userAgentFactory
    val ua: Broadcast[UserAgentAnalyzer] =
      spark.sparkContext.broadcast[UserAgentAnalyzer](theUserAgent)
    val df = spark.read
      .option("header", "false")
      .schema(schema)
      .csv(p.inputLocation.get)
      .withColumn(userAgentStruct, userAgentParserBroadcast(col(userAgent), ua))
      .select(id, userAgentStruct)
      .transform(userAgentExtractColumns(col(userAgentStruct), userAgentMap))
    val selection: Seq[String] = id +: userAgentMap.values.toSeq
    df.select(selection.head, selection.tail: _*)
      .coalesce(1)
      .orderBy(selection.head, selection.tail: _*)
      .show(truncate = false)
    /*
      .write
      .mode(SaveMode.Overwrite)
      .csv(s"/tmp/6.8")
     */
  }
}
