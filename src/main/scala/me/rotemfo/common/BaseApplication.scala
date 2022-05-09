package me.rotemfo.common

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

import java.time.format.DateTimeFormatter
import scala.util.{Failure, Try}

trait SparkApp[P <: Product] {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  /** main entry point for Spork code
    * @param p config parameters
    * @param spark SQLContext
    */
  protected def invoke(implicit p: P, spark: SQLContext): Unit

  /** get the required parser
    * @return OptionParser
    */
  protected def getParser: OptionParser[P]

  protected def getConfigVar(
      configVar: String,
      configPath: String = "/lib/spark/conf/spark-defaults.conf"
  ): String = {
    val applicationConf: Config = ConfigFactory.load(configPath)
    applicationConf.getString(configVar)
  }

  protected def configSparkSession(p: P, sparkSessionBuilder: SparkSession.Builder): Unit = {}
}

abstract class BaseApplication[P <: Product](p: P) extends SparkApp[P] {
  def main(args: Array[String]): Unit = {
    getParser.parse(args, p).foreach { p =>
      val sparkDefaultParallelism: String =
        try {
          getConfigVar("spark.default.parallelism")
        } catch {
          case _: Throwable => "200" // official default value of spark.default.parallelism
        }

      val sparkSessionBuilder = SparkSession
        .builder()
        // .enableHiveSupport()
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.broadcastTimeout", 6600)
        .config("spark.sql.shuffle.partitions", sparkDefaultParallelism)
      // .config("spark.sql.files.ignoreCorruptFiles", "true")

      configSparkSession(p, sparkSessionBuilder)

      val sparkSession        = sparkSessionBuilder.getOrCreate
      val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
      hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
      hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")

      val processResults: Try[Unit] = Try(invoke(p, sparkSession.sqlContext))
      sparkSession.sparkContext.getPersistentRDDs.values.foreach(_.unpersist())
      sparkSession.close()
      processResults match {
        case Failure(ex) => throw ex
        case _           =>
      }
    }
  }
}

abstract class LocalBaseApplication[P <: Product](p: P) extends BaseApplication[P](p) {
  override protected def configSparkSession(p: P, sparkSessionBuilder: SparkSession.Builder): Unit =
    sparkSessionBuilder.master("local[*]")
}

object BaseApplication {
  val dateHourPattern                             = "yyyy-MM-dd-HH"
  val dateHourPatternFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateHourPattern)
}
