package etl

import java.nio.file.{Paths, Files}
import org.apache.spark.{SparkConf, SparkContext}

object ETL extends App {

  if (args.length == 4) {
    val rawDatasetPath = args(2)
    if (Files.exists(Paths.get(rawDatasetPath))) {
      val sparkConf = new SparkConf()
        .setSparkHome(args(0))
        .setAppName("ETL")
        .setMaster(args(1))
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "serialization.Registrator")
        .set("spark.kryoserializer.buffer.mb", "50")

      val context = new SparkContext(sparkConf)

      // For transformation from song ID to song name. File available at
      // http://labrosa.ee.columbia.edu/millionsong/pages/getting-dataset at "1." under "Additional Files".
      val translationDataset = context.textFile("unique_tracks.txt")
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


      /* Transform! */
      val dataset = context.textFile(rawDatasetPath)

      // Case class, just for readability's sake
      case class Row(userId: String, songName: String, playCount: Int)
      val triplets = dataset.map(line => {
        val splitLine = line.split(",")
        val songId = splitLine(1)
        val songName =
          if (songIdToSongName.contains(songId))
            songIdToSongName(songId)
          else
            songId
        new Row(splitLine(0), songName, splitLine(2).toInt)
      })
      val playCountFilter = args(3).toInt
      val cleanTriplets = triplets.filter(_.playCount >= playCountFilter)
      val userSongGrouping = cleanTriplets.groupBy(_.userId)
      val userSongSets = userSongGrouping map {
        case (userId, userIdRowArray) =>
          userIdRowArray.map(r => (r.songName, r.playCount)).toSet
      }

      // Write transformed data to a file.
      val tupleSeparator = "<TUPSEP>"
      val formattedTupleSets = userSongSets.map(_.map {
        case (songName, playCount) => s"($songName$tupleSeparator$playCount)"
      })
      val fileContents = formattedTupleSets.map(_.mkString(separator))
      val transformedDataFile = "taste-profile-subset-transformed"
      fileContents.saveAsTextFile(transformedDataFile)

      context.stop()
      System.exit(0)
    }
    else
      println(s"The data set to transform ($rawDatasetPath) doesn't exist!")
  }
  else
    println("[spark home] [master URL] [dataset path]")
}
