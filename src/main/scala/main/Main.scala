package main

import org.apache.spark.SparkContext

import kmeans.SetKMeans
import java.nio.file.{Paths, Files}

object Main extends App {
  // Spark initialization
  val sparkHome = "/home/dash/prog/lang/scala/spark"
  val master = "local[4]"
  implicit val context = new SparkContext(master, "Song Set K-Means", sparkHome)

  val transformedDataFile = "transformed-subset"
  val songSeparator = "<SEP>"
  // Cached because K-Means is an iterative algorithm
  val trainingData = context.textFile(transformedDataFile).map(_.split(songSeparator).toSet).cache()

  val (kMeansResults, _) = SetKMeans.run (
    trainingData = trainingData,
    k = 10
  )

  val resultsFile = Paths.get("clustering-results")
  if (!Files.exists(resultsFile))
    Files.createFile(resultsFile)
  Files.write(resultsFile, kMeansResults.toString.getBytes)

  context.stop()
  System.exit(0) // Make sure everything stops
}