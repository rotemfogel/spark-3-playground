package me.rotemfo.stats

import me.rotemfo.common.{EmptyConfig, EmptyConfigParser, LocalBaseApplication}
import org.apache.spark.ml.clustering.{BisectingKMeans, GaussianMixture, KMeans, LDA}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SQLContext}
import scopt.OptionParser

object ClusteringApp
  extends LocalBaseApplication[EmptyConfig](EmptyConfig()) {

  override protected def invoke(p: EmptyConfig, spark: SQLContext): Unit = {
    def kMeans(dataset: DataFrame): Unit = {
      // Trains a k-means model.
      val kmeans = new KMeans().setK(2).setSeed(1L)
      val model = kmeans.fit(dataset)

      // Make predictions
      val predictions = model.transform(dataset)

      // Evaluate clustering by computing Silhouette score
      val evaluator = new ClusteringEvaluator()

      val silhouette = evaluator.evaluate(predictions)
      println(s"Silhouette with squared euclidean distance = $silhouette")

      // Shows the result.
      println("Cluster Centers: ")
      model.clusterCenters.foreach(println)
    }

    def bisectingKMeans(dataset: DataFrame): Unit = {
      // Trains a bisecting k-means model.
      val bkm = new BisectingKMeans().setK(2).setSeed(1)
      val model = bkm.fit(dataset)

      // Make predictions
      val predictions = model.transform(dataset)

      // Evaluate clustering by computing Silhouette score
      val evaluator = new ClusteringEvaluator()

      val silhouette = evaluator.evaluate(predictions)
      println(s"Silhouette with squared euclidean distance = $silhouette")

      // Shows the result.
      println("Cluster Centers: ")
      val centers = model.clusterCenters
      centers.foreach(println)
    }

    def gmm(dataset: DataFrame): Unit = {
      // Trains Gaussian Mixture Model
      val gmm = new GaussianMixture()
        .setK(2)
      val model = gmm.fit(dataset)

      // output parameters of mixture model model
      for (i <- 0 until model.getK) {
        println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
          s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
      }
    }

    def lda(): Unit = {
      // Loads data.
      val dataset = spark.read.format("libsvm")
        .load("data/mllib/sample_lda_libsvm_data.txt")

      // Trains a LDA model.
      val lda = new LDA().setK(10).setMaxIter(10)
      val model = lda.fit(dataset)

      val ll = model.logLikelihood(dataset)
      val lp = model.logPerplexity(dataset)
      println(s"The lower bound on the log likelihood of the entire corpus: $ll")
      println(s"The upper bound on perplexity: $lp")

      // Describe topics.
      val topics = model.describeTopics(3)
      println("The topics described by their top-weighted terms:")
      topics.show(false)

      // Shows the result.
      val transformed = model.transform(dataset)
      transformed.show(false)
    }

    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    kMeans(dataset)
    bisectingKMeans(dataset)
    gmm(dataset)
    lda()
  }

  override protected def getParser: OptionParser[EmptyConfig] = EmptyConfigParser
}
