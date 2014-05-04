package main

import org.apache.spark.{SparkConf, SparkContext}

import kmeans.SetKMeans
import java.nio.file.{Paths, Files}

object Main extends App {

  if (args.length == 3) {
    // Spark initialization
    val conf = new SparkConf()
               .setSparkHome(args(0))
               .setMaster(args(1))
               .setAppName("Song Set K-Means")
               .set("spark.executor.memory", "5g")
               .set("spark.cores.max", "4")

    implicit val context = new SparkContext(conf)

    val transformedDataFile = args(2)
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
  else
    println("You must enter the path to a spark installation, master URL, and transformed dataset!")
}