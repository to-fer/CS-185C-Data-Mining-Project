package kmeans

import org.apache.spark.rdd.RDD

class ClusteringResults(val clusters: Map[Int, RDD[Set[String]]],
                        val centroids: Seq[Set[String]],
                        val k: Int,
                        val iterations: Int,
                        val dataCount: Int) {
  override def toString = {
    val headerString = s"""
      |CLUSTERING RESULTS
      |K = $k
      |Iterations = $iterations
    """.stripMargin

    val clusterSizesAndPercentages = clusters.mapValues(rdd => {
      val count = rdd.count()
      (count, ((count / dataCount.toDouble) * 100).toInt)
    }).toArray.sortWith {
      case ((clusterIndex, _), (clusterIndex2, _)) => clusterIndex < clusterIndex2
    }

    val summaryString = "Summary:\n" + (clusterSizesAndPercentages.map {
      case (clusterIndex, (count, percentage)) => s"$clusterIndex: $count($percentage%)"
    }).mkString("\n") + "\n"

    val unclusteredDataPoints = dataCount - clusterSizesAndPercentages.map(_._1).sum
    val unclusteredString = s"Unclustered: $unclusteredDataPoints\n"

    var centroidString = "Centroids: \n"
    for (i <- 0 until centroids.length) {
      val centroidElementString = "{" + centroids(i).mkString(", ") + "}"
      centroidString += s"$i: $centroidElementString\n"
    }

    headerString + "\n" + summaryString + unclusteredString + "\n" + centroidString
  }

}