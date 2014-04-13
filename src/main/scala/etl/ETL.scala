import java.nio.file.{Paths, Files}
import org.apache.spark.SparkContext

object ETL extends App {
  val datasetToTransformPath = "1-4-triplets.csv"
  if (Files.exists(Paths.get(datasetToTransformPath))) {
    val sparkHome = "/home/dash/prog/lang/scala/lib/spark-0.9.0-incubating"
    val master = "local[4]"
    val context = new SparkContext(master, "ETL", sparkHome)

    val dataset = context.textFile(datasetToTransformPath)

    // For transformation from song ID to song name. File available at
    // http://labrosa.ee.columbia.edu/millionsong/pages/getting-dataset at "1." under "Additional Files".
    val songNameFile = context.textFile("unique_tracks.txt")
    val songNameFileSeparator = "<SEP>"
    val songIdToSongName = songNameFile.map(line => {
      line.split(songNameFileSeparator)
    }).collect {
      case splitLine: Array[String] if splitLine.length == 4 => {
        val songId = splitLine(1)
        val songName = splitLine(3)
        (songId -> songName)
      }
    }.toArray.toMap

    /* Transform! */
    // Case class, just for readability's sake
    case class Datum(userId: String, songId: String, playCount: Int)
    val triplets = dataset map (line => {
      val splitLine = line split ","
      val songId = splitLine(1)
      val songName =
        if (songIdToSongName.contains(songId))
          songIdToSongName(songId)
        else
          songId
      new Datum(splitLine(0), songName, splitLine(2).toInt)
    })

    val userSongArrayBuffers = triplets groupBy (_.userId)
    val userSongSets = (userSongArrayBuffers map {
      case (userId, userListeningHabits) => userListeningHabits.map(_.songId).toSet
    })

    // Write transformed data to a file.
    val fileContents = userSongSets.map(_.mkString(songNameFileSeparator))
    val transformedDataFile = "taste-profile-subset-transformed"
    fileContents.saveAsTextFile(transformedDataFile)

    context.stop()
    System.exit(0)
  }
  else
    println(s"The data set to transform ($datasetToTransformPath) doesn't exist!")

}
