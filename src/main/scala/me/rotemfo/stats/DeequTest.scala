package me.rotemfo.stats

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.anomalydetection.RelativeRateOfChangeStrategy
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.{ConstraintStatus, NamedConstraint}
import com.amazon.deequ.metrics.DistributionValue
import com.amazon.deequ.profiles.{NumericColumnProfile, StandardColumnProfile}
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import me.rotemfo.common.{EmptyConfig, EmptyConfigParser, LocalBaseApplication}
import org.apache.spark.sql.SQLContext
import scopt.OptionParser

object DeequTest extends LocalBaseApplication[EmptyConfig](EmptyConfig()) {

  override protected def invoke(implicit p: EmptyConfig, spark: SQLContext): Unit = {
    import spark.sparkSession.implicits._

    val orders = Seq(
      (1, "order#1", -3.9d, "CONFIRMED"),
      (2, "order#2", 3.9d, "CONFIRMED"),
      (3, "order#3", 23.9d, "CONFIRMED"),
      (4, "order#4", 34.9d, "PENDING"),
      (1, "order#1", -3.9d, "CONFIRMED"),
      (5, "order#5", 31.9d, "DELETED")
    ).toDF("id", "label", "amount", "status")

    val suggestionResult = ConstraintSuggestionRunner()
      .onData(orders)
      .addConstraintRules(Rules.DEFAULT)
      .run()

    val idColumnProfile = suggestionResult.columnProfiles("id")
    idColumnProfile.asInstanceOf[NumericColumnProfile].completeness equals 1d
    idColumnProfile.asInstanceOf[NumericColumnProfile].approximateNumDistinctValues equals 5
    idColumnProfile.asInstanceOf[NumericColumnProfile].minimum equals Some(1)
    idColumnProfile.asInstanceOf[NumericColumnProfile].maximum equals Some(5)
    val labelColumnProfile = suggestionResult.columnProfiles("label")
    labelColumnProfile.asInstanceOf[StandardColumnProfile].typeCounts equals Map(
      "Boolean"    -> 0,
      "Fractional" -> 0,
      "Integral"   -> 0,
      "Unknown"    -> 0,
      "String"     -> 6
    )
    val statusColumnProfile = suggestionResult.columnProfiles("status")
    val statusHistogram     = statusColumnProfile.asInstanceOf[StandardColumnProfile].histogram
    statusHistogram.isDefined equals true
    statusHistogram.get.values equals Map(
      "CONFIRMED" -> DistributionValue(4, 0.6666666666666666),
      "PENDING"   -> DistributionValue(1, 0.16666666666666666),
      "DELETED"   -> DistributionValue(1, 0.16666666666666666)
    )

    val metricsRepository = new InMemoryMetricsRepository()
    // Setup measures for the first time
    var nowKey = ResultKey(System.currentTimeMillis())
    VerificationSuite()
      .onData(orders)
      .addCheck(
        Check(CheckLevel.Error, "Ensure data quality")
          .hasSize(itemsCount => itemsCount == 5, Some("<SIZE>"))
      )
      .useRepository(metricsRepository)
      .saveOrAppendResult(nowKey)
      .run()

    // Here we try with 14 orders because we want to see the anomaly detection
    // fail because of the multiplied number of rows
    val ordersOneMinuteLater = Seq(
      (1, "order#1", -3.9d, "CONFIRMED"),
      (2, "order#2", 3.9d, "CONFIRMED"),
      (3, "order#3", 23.9d, "CONFIRMED"),
      (4, "order#4", 34.9d, "PENDING"),
      (1, "order#1", -3.9d, "CONFIRMED"),
      (5, "order#5", 31.9d, "DELETED"),
      (5, "order#5", 31.9d, "DELETED"),
      (5, "order#5", 31.9d, "DELETED"),
      (5, "order#5", 31.9d, "DELETED"),
      (5, "order#5", 31.9d, "DELETED"),
      (5, "order#5", 31.9d, "DELETED"),
      (5, "order#5", 31.9d, "DELETED"),
      (5, "order#5", 31.9d, "DELETED"),
      (5, "order#5", 31.9d, "DELETED")
    ).toDF("id", "label", "amount", "status")
    val ordersOneMinuteLaterKey = ResultKey(System.currentTimeMillis())
    val verificationResultOneMinuteLater = VerificationSuite()
      .onData(ordersOneMinuteLater)
      .useRepository(metricsRepository)
      .saveOrAppendResult(ordersOneMinuteLaterKey)
      // We expect at most 2x increase
      .addAnomalyCheck(RelativeRateOfChangeStrategy(maxRateIncrease = Some(2)), Size())
      .run()

    verificationResultOneMinuteLater.status equals CheckStatus.Warning
    // You can also visualize the metrics from the repository
    metricsRepository
      .load()
      .forAnalyzers(Seq(Size()))
      .getSuccessMetricsAsDataFrame(spark.sparkSession)
      .show()
    // +-------+--------+----+-----+-------------+
    //| entity|instance|name|value| dataset_date|
    //+-------+--------+----+-----+-------------+
    //|Dataset|       *|Size|  6.0|1579069527000|
    //|Dataset|       *|Size| 14.0|1579069527611|
    //+-------+--------+----+-----+-------------+

    nowKey = ResultKey(System.currentTimeMillis())
    val verificationResult = VerificationSuite()
      .onData(orders)
      .addCheck(
        Check(CheckLevel.Error, "Ensure data quality")
          // Some<SIZE> --> custom prefix that you can add to the validation result message
          .hasSize(itemsCount => itemsCount == 5, Some("<SIZE>"))
          // Ensure uniqueness of the order id
          .isComplete("id")
          .isUnique("id")
          // Ensure completness (NOT NULL) of productName which is missing in the dataset!
          .isComplete("productName")
          // Ensure all statuses are contained in the array
          .isContainedIn("status", Array("CONFIRMED", "DELETED", "PENDING"))
          // Ensure that the max amount is positive and at most 100
          .isNonNegative("amount")
          .hasMax("amount", amount => amount == 100d)
      )
      .run()
    verificationResult.status equals CheckStatus.Error

    val resultsForAllConstraints = verificationResult.checkResults
      .flatMap { case (_, checkResult) => checkResult.constraintResults }
    val successfulConstraints = resultsForAllConstraints
      .filter(result => result.status == ConstraintStatus.Success)
      .map(result => result.constraint.asInstanceOf[NamedConstraint].toString())
    successfulConstraints.size equals 2
    Array(
      "CompletenessConstraint(Completeness(id,None))",
      "ComplianceConstraint(Compliance(status contained in CONFIRMED,DELETED,PENDING,`status` IS " +
        "NULL OR `status` IN ('CONFIRMED','DELETED','PENDING'),None))"
    ).foreach(l => successfulConstraints.toSeq.contains(l))

    val failedConstraints = resultsForAllConstraints
      .filter(result => result.status != ConstraintStatus.Success)
      .map(result => result.constraint.asInstanceOf[NamedConstraint].toString())
    failedConstraints.size equals 5
    Array(
      "SizeConstraint(Size(None))",
      "UniquenessConstraint(Uniqueness(List(id)))",
      "CompletenessConstraint(Completeness(productName,None))",
      "ComplianceConstraint(Compliance(amount is non-negative,COALESCE(amount, 0.0) >= 0,None))",
      "MaximumConstraint(Maximum(amount,None))"
    ).foreach(l => failedConstraints.toSeq.contains(l))
  }

  override protected def getParser: OptionParser[EmptyConfig] = EmptyConfigParser
}
