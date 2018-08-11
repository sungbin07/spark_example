import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 실행 파라미터
// example local ${TARGET_FILE} ${DESTINATION_DIR}
// local -> thread count
// local : default = 1 ( local[1] )
// local[2] : thread = 2 ( par-xx file 2개 생김)
object WordCount {

  def main(args: Array[String]): Unit = {

    require(args.length == 3, "Usage: WordCount <Master> <Input> <Output>")
    println(args.mkString(", "))

    val sc = Spark.getSparkContext("WordCount")

    val inputRDD = getInputRDD(sc, args(1))

    val resultRDD = process(inputRDD)

    handleResult(resultRDD, args(2))
  }

  def getInputRDD(sc: SparkContext, input: String): RDD[String] = {
    // make RDD object by file read
    sc.textFile(input)
  }

  def process(inputRDD: RDD[String]): RDD[(String, Int)] = {
    // tokenize by space
    val words = inputRDD.flatMap(str => str.split(" "))
    val wcPair = words.map((_, 1))
    // reduce
    wcPair.reduceByKey(_ + _)
  }

  def handleResult(resultRDD: RDD[(String, Int)], output: String): Unit = {
    resultRDD.saveAsTextFile(output)
  }
}