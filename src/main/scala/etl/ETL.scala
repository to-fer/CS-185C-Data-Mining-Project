package etl

import java.nio.file.{Paths, Files}
import org.apache.spark.SparkContext

/**
 * Performs ETL on The Echo Nest Taste Profile Subset, as described in
 * http://sjsubigdata.wordpress.com/2014/02/08/nfl-anesidora/
 *
 * Usage:
 *  [spark home directory] [master URL] [untransformed dataset path]
 */
object ETL extends App {
  if (args.length == 3) {
    val rawDatasetPath = args(2)
    if (Files.exists(Paths.get(rawDatasetPath))) {
      val sparkHome = args(0)
      val appName = "ETL"
      val masterURL = args(1)
      val context = new SparkContext(masterURL, appName, sparkHome)

      /* Transform! */
      val dataset = context.textFile(rawDatasetPath)

      // Case class, just for readability's sake
      case class MaybeBadRow(userId: String, songID: String)
      case class GoodRow(userId: String, songName: String)
      val doubles = dataset.map(line => {
        val splitLine = line.split(",")
        val songID = splitLine(1)
        new MaybeBadRow(splitLine(0), songID)
      })

      // For ignoring song IDs that were mismatched
      val errorDataset = context.textFile("sid_mismatches.txt")
      val erroneousSongIDs = errorDataset.map(line => {
        val indOfLt = line.indexOf("<")
        val indOfSpace = line.indexOf(" ", indOfLt)
        val songId = line.substring(indOfLt + 1, indOfSpace)
        songId
      }).toArray.toSet

      val correctDoubles = doubles.filter(row => !erroneousSongIDs.contains(row.songID))

      // For transformation of song ID to song name. File available at
      // http://labrosa.ee.columbia.edu/millionsong/pages/getting-dataset at "1." under "Additional Files".
      val translationDataset = context.textFile("translation-dataset")
      val separator = "<SEP>"
      val translationDatasetSplitLines =
        translationDataset.map(_.split(separator))
      val songIdToSongName = translationDatasetSplitLines.collect {
        case splitLine: Array[String] if splitLine.length == 4 => {
          val songId = splitLine(1)
          val songName = splitLine(3)
          (songId -> songName)
        }
      }.toArray.toMap

      val songNameDoubles = correctDoubles.map(row => {
        val songName =
          if (songIdToSongName.contains(row.songID))
            songIdToSongName(row.songID)
          else
            row.songID
        new GoodRow(row.userId, songName)
      })
      val userSongGrouping = songNameDoubles.groupBy(_.userId)
      val userSongSets = userSongGrouping map {
        case (userId, userIdRowArray) =>
          userIdRowArray.map(r => r.songName).toSet
      }

      // Write transformed data to a file.
      val fileContents = userSongSets.map(_.mkString(separator))
      val transformedDataFile = "transformed"
      fileContents.saveAsTextFile(transformedDataFile)

      context.stop()
      System.exit(0)
    }
    else
      println(s"The data set to transform ($rawDatasetPath) doesn't exist!")
  }
  else {
    println("Incorrect number of arguments.")
    println("")
    println("ETL usage:")
    println("[spark home directory] [master URL] [untransformed dataset path]")
  }

}
