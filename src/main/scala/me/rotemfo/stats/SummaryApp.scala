package me.rotemfo.stats

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.anomalydetection.RelativeRateOfChangeStrategy
import com.amazon.deequ.checks.CheckStatus.Success
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import me.rotemfo.common.{ParquetReaderApplication, ReaderConfig}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.util.Random

object SummaryApp extends ParquetReaderApplication {

  private lazy val statsSchema = StructType(Seq(
    StructField("entity", StringType, nullable = false),
    StructField("instance", StringType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("value", DoubleType, nullable = true)
  ))

  override protected def invoke(p: ReaderConfig, spark: SQLContext): Unit = {
    lazy val session: SparkSession = spark.sparkSession

    def stats(): Unit = {
      val df = spark.read.parquet(p.inputLocation.get).select("event_type", "event_source", "event_action")
      val columns = df.schema.fieldNames
      val emptyStatsDf: DataFrame = session.createDataFrame(spark.sparkContext.emptyRDD[Row], statsSchema)

      val statsDf = columns.foldLeft(emptyStatsDf) { (_, column) =>
        val analysisResult: AnalyzerContext = {
          AnalysisRunner
            // data to run the analysis on
            .onData(df)
            // define analyzers that compute metrics
            .addAnalyzer(ApproxQuantile(column, 0.5))
            .addAnalyzer(Mean(column))
            .addAnalyzer(Maximum(column))
            .addAnalyzer(Minimum(column))
            .addAnalyzer(Histogram(column))
            .addAnalyzer(CountDistinct(column))
            .run()
        }
        successMetricsAsDataFrame(session, analysisResult)
      }
      statsDf.show(100, truncate = false)
    }

    def anomalyDetection(): Unit = {
      val yesterday = Range.inclusive(1, 100).map(_ => Random.nextDouble)
      val today = yesterday.drop(20).map(_ => Random.nextDouble)
      import spark.implicits._
      val todayDf = spark.sparkContext.parallelize(today).toDF("val")
      val yesterdayDf = spark.sparkContext.parallelize(yesterday).toDF("val")

      val todayKey = ResultKey(todayDf.hashCode())
      val yesterdaysKey = ResultKey(yesterdayDf.hashCode())

      val metricsRepository = new InMemoryMetricsRepository()

      VerificationSuite()
        .onData(yesterdayDf)
        .useRepository(metricsRepository)
        .saveOrAppendResult(yesterdaysKey)
        .addAnomalyCheck(
          RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)),
          Size())
        .run()

      val verificationResult =
        VerificationSuite()
          .onData(todayDf)
          .useRepository(metricsRepository)
          .saveOrAppendResult(todayKey)
          .addAnomalyCheck(
            RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)),
            Size())
          .run()

      if (verificationResult.status != Success) {
        println("Anomaly detected in the Size() metric!")

        metricsRepository
          .load()
          .forAnalyzers(Seq(Size()))
          .getSuccessMetricsAsDataFrame(session)
          .show()
      }
    }

    if (false) {
      anomalyDetection()
      stats()
    }
  }
}
