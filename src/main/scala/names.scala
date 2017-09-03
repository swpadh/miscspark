import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object names {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("names").getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val base = "src/main/resources/"

    //val df = sparkSession.read.json(base + "rows.json")
    //df.createOrReplaceTempView("names")

    // df.printSchema()

    //val sqlDF = sparkSession.sql("SELECT * FROM names")

    //sqlDF.show()

    sc.stop
  }
}