package kmeans

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import SparkContext._

object SetKMeans {
  // TODO add convergence detection
  // WARNING: SetKMeansModel is returned, but is a class that has yet to be implemented. Do not use.
  def run(trainingData: RDD[Map[String, Double]], k: Int = 10, maxIterations: Int = 50)
         (implicit sparkContext: SparkContext): (ClusteringResults, SetKMeansModel) = {
    var centroids = 
      trainingData.takeSample(false, k, System.currentTimeMillis.toInt)
    var clusters = Map.empty[Int, RDD[Map[String, Double]]]

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
    } while(convergenceSim > sumOfSim && maxIterations > currentIteration)

    val results = new ClusteringResults(k = k,
                                        clusters = clusters,
                                        centroids = centroids,
                                        iterations = currentIteration,
                                        dataCount = trainingData.count().toInt)
    (results, new SetKMeansModel)
  }

  private def similarity(mapA: Map[String, Double], mapB: Map[String, Double]): Double = {
    val setA = mapA.keys.toSet
    val setB = mapB.keys.toSet

    val intersection = setA.intersect(setB)
    val union = setA.union(setB)
    val jaccardIndex = intersection.size/union.size.toDouble

    val playcountSimilarity =
      if (!intersection.isEmpty)
        (intersection.map(songName => {
          val songCountA = mapA(songName)
          val songCountB = mapB(songName)
          if (songCountA > songCountB) songCountB/songCountA.toDouble
          else songCountA/songCountB.toDouble
        }).sum)/intersection.size
      else 0.0

    jaccardIndex * playcountSimilarity
  }

  /**
   * Finds the index of the centroid that a data point is closest to. If the data point's distance to each of the centroids is
   * undefined, then we randomly choose a centroid index as the closest.
   *
   * @param dataPoint the data point to compare to the centroids
   * @param centroids the cluster centroids
   * @return the centroid that dataPoint is closest to.
   */
  private def closestCentroid(dataPoint: Map[String, Double], centroids: Seq[Map[String, Double]]): Option[Int] = {
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
  private def clusterData(data: RDD[Map[String, Double]], centroids: Seq[Map[String, Double]])
                         (implicit sparkContext: SparkContext): Map[Int, RDD[Map[String, Double]]] = {
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
          clusters + (i -> sparkContext.makeRDD(List.empty[Map[String, Double]]))
    clusters
  }

  /**
   * Calculates the new centroids of each cluster by taking their set-average.
   *
   * @param clusters the clusters to find the new centroids of
   * @return the new centroids of the clusters
   */
  private def findNewCentroids(clusters: Map[Int, RDD[Map[String, Double]]],
                               k: Int): Array[Map[String, Double]] = {
    val newCentroids = new Array[Map[String, Double]](k)
    clusters foreach { case (clusterNumber, cluster) => {
      val newCentroid =
      if (cluster.count != 0 )
        average(cluster)
      else
        Map.empty[String, Double]
      newCentroids(clusterNumber) = newCentroid
    } }
    newCentroids
  }

  /**
   * Calculates the average data point in the cluster by considering both the occurrence count of a song in
   * the cluster as well as its average play count. Uses factors between 0 and 1 to determine occurrence count
   * and play count average requirements that all cluster elements must meet in order to be included in the
   * average.
   *
   * @param cluster the cluster to find the average element of
   * @param averageFactor the factor to multiply the most common occurrence count by to get a lower
   *                      bound on the occurrence count of every element to be included in the average.
   * @param playcountFactor the factor to multiply the greatest play count average by to get a lower bound
   *                        on the play count average of every element to be included in the average
   * @return a mapping from song name to (average) play count of all of the most common and well-liked
   *         songs in this cluster.
   */
  private def average(cluster: RDD[Map[String, Double]],
                      averageFactor: Double = 0.3,
                      playcountFactor: Double = 0.5): Map[String, Double] = {

    val groupedBySongName = cluster.flatMap(map => map).groupBy {
      case (songName, playCount) => songName
    }
    val counts = groupedBySongName.map {
      case (songName, playCountList) => {
        val playCountSum = playCountList.map(_._2).sum
        val occurrenceCount = playCountList.size
        val playCountAverage = playCountSum/occurrenceCount.toDouble
        (songName, occurrenceCount, playCountAverage)
      }
    }
    val mostCommonSongCount = counts.fold(counts.first)(
      (mostCommon, visited) => if (visited._2 > mostCommon._2) visited else mostCommon
    )._2
    val occurrenceCountRequirement = mostCommonSongCount * averageFactor
    val commonSongs = counts.filter {
      case (_, occurrenceCount, _) => occurrenceCount >= occurrenceCountRequirement
    }

    /*
    val mostLikedSongPlayCountAverage = commonSongs.fold(commonSongs.first())(
      (mostLiked, visited) => if (visited._3 > mostLiked._3) visited else mostLiked
    )._3
    val likeRequirement = mostLikedSongPlayCountAverage * playcountFactor
    val likedCommonSongs = commonSongs.filter {
      case (_, _, playCountAverage) => playCountAverage >= likeRequirement
    }
    // Replace line below with likedCommonSongs.map instead of using commonSongs.map if you
    // want to use play count consideration when calculating the average here.
    */
    val averageClusterElements = commonSongs.map {
      case (songName, _, playCountAverage) => (songName, playCountAverage)
    }
    averageClusterElements.toArray.toMap
  }
}