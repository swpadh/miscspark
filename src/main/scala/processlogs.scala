import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.concurrent._
import org.apache.spark.sql.functions._

object processlogs {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("processlogs").getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val base = "src/main/resources/"

    import sparkSession.implicits._

    val df = sparkSession.read.textFile(base + "log1.txt").map(line => ApacheAccessLog.parseLogLine(line)).cache()

    df.createOrReplaceTempView("logs")
    
    val col = df.col("contentSize");

    val stats: Row = df.agg(sum(col), count(col), max(col), min(col)).head

    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      stats.getLong(0) / stats.getLong(1),
      stats(2),
      stats(3)))

    sc.stop
  }
}