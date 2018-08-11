import org.apache.spark.{SparkConf, SparkContext}

object Spark {

  def getSparkContext(appName: String): SparkContext = {
    // create config
    val conf = new SparkConf().setAppName(appName).setMaster("local[*]")
    // create spark context
    new SparkContext(conf)
  }
}
