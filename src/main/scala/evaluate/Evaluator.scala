package evaluate

import util.{DatasetUtil, SparkContextUtil}
import org.apache.spark.SparkContext
import kmeans.SetKMeans
import java.nio.file.{Files, Paths}

object Evaluator extends App {

  def apply(datasetPath: String, evalOutputPath: String, k: Int)(implicit context: SparkContext) = {
    val songSeparator = "<SEP>"
    val trainingRDD = DatasetUtil.trainingData(datasetPath, 1.0, songSeparator)
    val results = SetKMeans.run(trainingRDD, k)

    val artistDataset = context.textFile("translation-dataset").map(_.split(songSeparator))
    val songToArtist = artistDataset.collect {
      // There are some errors in the dataset that result in not every row having the number of
      // attributes that it should.
      case line: Array[String] if line.length == 4 => {
        val artistName = line(2)
        val songName = line(3)
        (songName -> artistName)
      }
    }.toArray.toMap

    val artistClusters = results.clusters.map {
      case (clusterIndex, cluster) => {
        val artistCluster = cluster.map(songSet => {
          val artistList = songSet.map(songToArtist)
          artistList.toSet
        })
        (clusterIndex -> artistCluster)
      }
    }

    val artistClusterCentroids = artistClusters.map {
      case (clusterIndex, cluster) => {
        val clusterArtistNames = cluster.flatMap(artistSet => artistSet).toArray.toSet
        (clusterIndex, clusterArtistNames)
      }
    }

    val artistData = context.makeRDD(artistClusters.flatMap {
      case (clusterIndex, cluster) => {
        cluster.map((_, clusterIndex)).toArray
      }
    }.toArray)

    val errorCount = artistData.filter { case (artistSet, oldCentroidIndex) => {
      val centroidSimilarities = artistClusterCentroids.map {
        case (centroidIndex, centroid) => {
          (SetKMeans.similarity(centroid, artistSet), centroidIndex)
        }
      }
      val mostSimilarCentroid = (centroidSimilarities.maxBy {
        case (similarity, centroidIndex) => similarity
      })._2

      mostSimilarCentroid == oldCentroidIndex
    }}.count

    val correct = results.dataCount - errorCount
    val accuracy = correct/results.dataCount.toDouble
    val accuracyPercentage = accuracy * 100

    val outputString = "%2.2f" format accuracyPercentage
    val outputPath = Paths.get(evalOutputPath)
    Files.deleteIfExists(outputPath)
    Files.write(outputPath, outputString.getBytes)

    context.stop()
    System.exit(0)
  }

  if (args.length == 5) {
    implicit val context = SparkContextUtil.newSparkContext(args(0), args(1))
    apply(args(2), args(4), args(3).toInt)
  }
  else
    println("Invalid number of arguments!")
}
