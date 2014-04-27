package kmeans

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

object SetKMeans {
  // TODO add convergence detection
  // WARNING: SetKMeansModel is returned, but is a class that has yet to be implemented. Do not use.
  def run(trainingData: RDD[Set[String]], k: Int = 10)
         (implicit sparkContext: SparkContext): (ClusteringResults, SetKMeansModel) = {
    var centroids = 
      trainingData.takeSample(false, k, System.currentTimeMillis.toInt)
    var clusters = Map.empty[Int, RDD[Set[String]]]

    val convergenceSim = k
    var currentIteration = 0
    var sumOfSim = 0.0
    do {
      currentIteration += 1

      clusters = clusterData(trainingData, centroids)
      val newCentroids = findNewCentroids(clusters, k)

      sumOfSim = 0.0
      for (i <- 0 until k) {
        val distance = similarity(centroids(i), newCentroids(i))
        sumOfSim += distance
      }
      centroids = newCentroids
    } while(convergenceSim > sumOfSim)

    val results = new ClusteringResults(k = k,
                                        clusters = clusters,
                                        centroids = centroids,
                                        iterations = currentIteration,
                                        dataCount = trainingData.count().toInt)
    (results, new SetKMeansModel)
  }

  private def similarity(setA: Set[String], setB: Set[String]) = {
    val numOfCommonElements = setA.intersect(setB).size
    val numOfTotalElements = setA.union(setB).size
    numOfCommonElements/numOfTotalElements.toDouble
  }

  /**
   * Finds the index of the centroid that a data point is closest to. If the data point's distance to each of the centroids is
   * undefined, then we randomly choose a centroid index as the closest.
   *
   * @param dataPoint the data point to compare to the centroids
   * @param centroids the cluster centroids
   * @return the centroid that dataPoint is closest to.
   */
  private def closestCentroid
  (dataPoint: Set[String], centroids: Seq[Set[String]]): Option[Int] = {
    val similarities = centroids.zipWithIndex map {
      case (centroid, index) => (similarity(dataPoint, centroid), index)
    }
    val closestCentroidIndex =
      if (similarities.forall {
        case (similarity, centroidIndex) => similarity == 0.0
      })
      // Ignore data points that have nothing in common with any centroid.
        None
      else
        Some((similarities.maxBy {
          case (similarity, centroidIndex) => similarity
        })._2)
    closestCentroidIndex
  }

  /**
   * Partitions data into clusters based on their distance from each cluster centroid.
   *
   * @param data the data to be partitioned into clusters
   * @param centroids the centroids of each cluster
   * @return a mapping from centroid index to that centroid's clustered data
   */
  // TODO always makes 2 large clusters and 2 small ones when k = 4. Bug, or consequence of a small K?
  private def clusterData(data: RDD[Set[String]], centroids: Seq[Set[String]])
                         (implicit sparkContext: SparkContext): Map[Int, RDD[Set[String]]] = {
    val centroidIndexToDataPoint = data.keyBy(
      closestCentroid(_, centroids)
    ).filter {
      case (indexOption, dataPoint) => indexOption.isDefined
    }.map {
      case (indexOption, dataPoint) => (indexOption.get, dataPoint)
    }

    val groupedByClusterIndex = centroidIndexToDataPoint groupBy {
      case (centroidIndex, _) => centroidIndex
    }
    var clusters = ((groupedByClusterIndex map {
      case (clusterIndex: Int, clusterIndexToS) => {
        val clusterSets = clusterIndexToS map {
          case (_, set) => set
        }
        (clusterIndex -> clusterSets)
      }
    }).toArray map {
      case (clusterIndex, clusterSets) =>
        (clusterIndex -> sparkContext.makeRDD(clusterSets))
    }).toMap

    for (i <- 0 until centroids.length)
      if (!clusters.contains(i))
        clusters =
          clusters + (i -> sparkContext.makeRDD(List.empty[Set[String]]))
    clusters
  }

  /**
   * Calculates the new centroids of each cluster by taking their set-average.
   *
   * @param clusters the clusters to find the new centroids of
   * @return the new centroids of the clusters
   */
  private def findNewCentroids(clusters: Map[Int, RDD[Set[String]]],
                               k: Int): Array[Set[String]] = {
    val newCentroids = new Array[Set[String]](k)
    clusters foreach { case (clusterNumber, cluster) => {
      val newCentroid =
      if (cluster.count != 0 )
        average(cluster)
      else
        Set.empty[String]
      newCentroids(clusterNumber) = newCentroid
    } }
    newCentroids
  }

  /**
   * Calculates the average of a collection of sets. This set-average is defined as the set consisting of
   * the most common elements among all of the given sets. We find these elements by finding the most
   * common element among all the sets, then defining a count requirement for the other elements among
   * all the sets. The number of occurrences of an element must be equal to or greater than this count
   * requirement in order to be included in the average set.
   *
   * @param cluster the cluster to find the average of
   * @param averageThreshold the percentage of the number of occurrences of the most common set to
   *                         use as the count requirement.
   * @return the set-average of the cluster
   */
  private def average(cluster: RDD[Set[String]],
                      averageThreshold: Double = 0.30): Set[String] = {
    val clusterSetElements = cluster flatMap (set => set)

    val clusterSetElementCounts = clusterSetElements.map((_, 1))
                                                    .reduceByKey(_ + _)
    val mostCommonElementCount = clusterSetElementCounts.fold(clusterSetElementCounts.first)(
      (mostCommonSong, song) => if (song._2 > mostCommonSong._2) song else mostCommonSong
    )._2
    val countRequirement = mostCommonElementCount * averageThreshold
    val averageElements = clusterSetElementCounts.collect {
      case (song, occurrenceCount)
        if (occurrenceCount >= countRequirement) => song
    }
    averageElements.toArray.toSet
  }
}