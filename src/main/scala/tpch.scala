import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

object tpch {

  case class Part(partid: Int, partdesc: String, partname: String, partweight: Int, price: Double)
  case class Supp(suppid: Int, city: String, suppname: String)
  case class PartSupp(suppid: Int, availableqty: Int, partid: Int, supplycost: Double)
  case class PartS(partid: Int, partdesc: String, partname: String, partweight: Int, price: Double, suppid: Int, availableqty: Int, supplycost: Double)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")

    val sparkSession = SparkSession.builder().master("local[*]").appName("tpch").config(conf).getOrCreate();

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    val partdata = sc.cassandraTable[Part]("tpch", "part")
    val suppdata = sc.cassandraTable[Supp]("tpch", "supp")
    val partsuppdata = sc.cassandraTable[PartSupp]("tpch", "partsupp")

    val partPairRDD = partdata.map(Part => (Part.partid, Part))
    val partsuppPairRDD = partsuppdata.map(PartSupp => (PartSupp.partid, PartSupp))
    val suppPairRDD = suppdata.map(Supp => (Supp.suppid, Supp))

    val tmpPartS = partPairRDD.join(partsuppPairRDD).map(x => (x._2._2.suppid,
      PartS(x._2._1.partid, x._2._1.partdesc, x._2._1.partname, x._2._1.partweight, x._2._1.price, x._2._2.suppid, x._2._2.availableqty, x._2._2.supplycost)))

    val partSupp = tmpPartS.join(suppPairRDD).map(x => (
      x._2._1.partid, x._2._1.partdesc, x._2._1.partname, x._2._1.partweight, x._2._1.price, x._2._1.availableqty, x._2._1.supplycost, x._2._2.suppid, x._2._2.city, x._2._2.suppname))

    partSupp.foreach(println)

    import sparkSession.implicits._

    val dfpart = sparkSession.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "tpch", "table" -> "part"))
      .load.createOrReplaceTempView("part")

    val dfpartsupp = sparkSession.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "tpch", "table" -> "partsupp"))
      .load.createOrReplaceTempView("partsupp")

    val dfsupp = sparkSession.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "tpch", "table" -> "supp"))
      .load.createOrReplaceTempView("supp")

    val dframe = sparkSession.sql("SELECT * from part inner join partsupp on part.partid = partsupp.partid inner join supp on partsupp.suppid = supp.suppid")
    dframe.show

    sc.stop
  }
}