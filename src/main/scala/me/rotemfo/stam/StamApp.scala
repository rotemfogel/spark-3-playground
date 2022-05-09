package me.rotemfo.stam

import me.rotemfo.common.{ParquetReaderApplication, ReaderConfig}
import org.apache.spark.sql.functions.{col, lit, min}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Date
import java.time.{LocalDate, LocalDateTime}

object StamApp extends ParquetReaderApplication {

  override protected def invoke(p: ReaderConfig, spark: SQLContext): Unit = {
    val start = LocalDate.of(2021, 1, 1)
    val now = LocalDateTime.now()
    val end = now.toLocalDate
    val df = spark.read.table("dbl.page_events").where(
      col("category").isin("page_view", "page_event") &&
        col("to_copy") === lit(1) &&
        col("date_").between(Date.valueOf(start), Date.valueOf(end)))
      .select("date_", "user_id", "machine_cookie", "machine_cookie_int")
      .where(
        col("user_id") > lit(0) &&
          col("machine_cookie").isNotNull &&
          col("machine_cookie_int").isNotNull)
      .repartitionByRange(24, col("user_id"), col("machine_cookie"))

    df.groupBy("user_id", "machine_cookie", "machine_cookie_int")
      .agg(min("date_").as("first_seen"))
      .withColumn("date_", lit(Date.valueOf(end)))
      .withColumn("hour", lit(UTF8String.fromString(f"${now.getHour}%02d")))
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("date_", "hour")
      .parquet("hdfs:///user_id_machine_cookie_map")
  }
}
