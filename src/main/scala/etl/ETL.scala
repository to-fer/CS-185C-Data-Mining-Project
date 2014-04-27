import java.nio.file.{Paths, Files}
import org.apache.spark.SparkContext

object ETL extends App {
  val rawDatasetPath = "1-4-triplets.csv"
  if (Files.exists(Paths.get(rawDatasetPath))) {
    val sparkHome = "/home/dash/prog/lang/scala/spark"
    val appName = "ETL"
    val masterURL = "local[4]"
    val context = new SparkContext(masterURL, appName, sparkHome)

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
    val userSongGrouping = triplets.groupBy(_.userId)
    val userSongSets = userSongGrouping map {
      case (userId, userIdRowArray) =>
        userIdRowArray.map(_.songName).toSet
    }

    // Write transformed data to a file.
    val fileContents = userSongSets.map(_.mkString(separator))
    val transformedDataFile = "taste-profile-subset-transformed"
    fileContents.saveAsTextFile(transformedDataFile)

    context.stop()
    System.exit(0)
  }
  else
    println(s"The data set to transform ($rawDatasetPath) doesn't exist!")
}
