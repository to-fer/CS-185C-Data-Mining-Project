package main

import org.apache.spark.{SparkConf, SparkContext}

import kmeans.SetKMeans
import java.nio.file.{Paths, Files}
import org.apache.spark.storage.StorageLevel
import util.{DatasetUtil, SparkContextUtil}

object KMeansDriver extends App {
  def run(datasetPath: String, fractionTrainingData: Double = 0.1, k: Int = 10)(implicit context: SparkContext) = {
    val songSeparator = "<SEP>"
    val trainingData = DatasetUtil.trainingData(datasetPath, fractionTrainingData, songSeparator)

    val kMeansResults = SetKMeans.run(trainingData = trainingData, k = k)

    val resultsFile = Paths.get("clustering-results")
    if (!Files.exists(resultsFile))
      Files.createFile(resultsFile)
    Files.write(resultsFile, kMeansResults.toString.getBytes)

    context.stop()
    System.exit(0) // Make sure everything stops
  }

  if (args.length == 4) {
    implicit val context = SparkContextUtil.newSparkContext(args(0), args(1))
    run(datasetPath = args(2), 1, args(3).toInt)
  }
  else
    println("You must enter the path to a spark installation, master URL, and transformed dataset!")
}