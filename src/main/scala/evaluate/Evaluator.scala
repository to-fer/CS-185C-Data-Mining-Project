package evaluate

import util.{DatasetUtil, SparkContextUtil}
import org.apache.spark.SparkContext
import kmeans.SetKMeans
import java.nio.file.{Files, Paths}
import scala.util.Random

object Evaluator extends App {

  def apply(datasetPath: String, evalOutputPath: String, k: Int)(implicit context: SparkContext) = {
    val songSeparator = "<SEP>"
    val trainingRDD = DatasetUtil.trainingData(datasetPath, 1.0, songSeparator)
    val results = SetKMeans.run(trainingRDD, k)

    val sampleSize = 5
    val representativeDataPoints = results.clusters.map {
      case (clusterIndex, cluster) => {
        val sample = cluster.takeSample(false, sampleSize, System.currentTimeMillis.toInt)
        val songSamples = sample.map(sampleSet => {
          val setSize = sampleSet.size
          val sampleArray = sampleSet.toArray
          if (setSize < sampleSize)
            sampleSet.toArray
          else {
            var sampledSongsArray = Array.empty[String]
            while (sampledSongsArray.size < sampleSize) {
              val randInd = Random.nextInt(setSize)
              val randomSong = sampleArray(randInd)
              if (!sampledSongsArray.contains(randomSong))
                sampledSongsArray = sampledSongsArray ++ Array(randomSong)
            }
            sampledSongsArray
          }
        })
        (clusterIndex, songSamples)
      }
    }

    val outputString = representativeDataPoints.map {
      case (clusterIndex, clusterSample) => {
        val sampleString = clusterSample.map(_.mkString(",")).mkString("\n")
        s"$clusterIndex:\n$sampleString"
      }
    }.mkString("\n\n")
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
