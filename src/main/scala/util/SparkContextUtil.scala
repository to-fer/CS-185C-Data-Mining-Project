package util

import org.apache.spark.{SparkContext, SparkConf}

object SparkContextUtil {

  def newSparkContext(sparkHome: String, masterURL: String) = {
    val conf = new SparkConf()
      .setSparkHome(sparkHome)
      .setMaster(masterURL)
      .setAppName("Song Set K-Means")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "serialization.Registrator")
      .set("spark.kryoserializer.buffer.mb", "50")
    new SparkContext(conf)
  }

}
