import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.concurrent._

object processudf {
  case class Location(id: Int, end_date: java.sql.Timestamp, start_date: java.sql.Timestamp, city: String, state: String)
 
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("processudf").getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val base = "src/main/resources/"

    import sparkSession.implicits._

    val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter", "|").option("inferSchema", "true").load(base + "dataset.txt")

    df.printSchema()
    df.show

    val locdataset = df.map {
      case Row(id: Int, end_date: java.sql.Timestamp, start_date: java.sql.Timestamp, location: String) => {
        val loc = location.asInstanceOf[String].split("-")
        Location(id, end_date, start_date,loc(0), loc(1))
      }
    }

    locdataset.show
   
    val encoder = Encoders.tuple(Encoders.scalaInt, Encoders.scalaLong,Encoders.STRING, Encoders.STRING)

    val locdataset2 = df.map {
      case Row(id: Int, end_date: java.sql.Timestamp, start_date: java.sql.Timestamp, location: String) => {
        val loc = location.asInstanceOf[String].split("-")
        val diff = end_date.getTime - start_date.getTime
        val ndays = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS)
        (id, ndays,loc(0), loc(1))

      }
    }(encoder)

    locdataset2.show

    sparkSession.udf.register("NDAYS", {(end_date: java.sql.Timestamp, start_date: java.sql.Timestamp) => 
                                val diff = end_date.getTime - start_date.getTime
                                TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS)})
   
    locdataset.createOrReplaceTempView("LocationDays") 
    
    val dframe = sparkSession.sql("SELECT id,NDAYS(end_date, start_date) as ndays,city, state from LocationDays")
    dframe.show

    sc.stop
  }
}