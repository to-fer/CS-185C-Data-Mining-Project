package util

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object DatasetUtil {
  def trainingData(datasetPath: String, fractionTrainingData: Double = 0.1, songSeparator: String)
                  (implicit context: SparkContext): RDD[Set[String]] = {
    val datasetSample = context.textFile(datasetPath).sample(false, fractionTrainingData, System.currentTimeMillis.toInt)
    val trainingData = datasetSample.map(_.split(songSeparator).toSet)
    trainingData.persist(StorageLevel.MEMORY_ONLY_SER)
  }
}
