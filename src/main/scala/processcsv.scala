import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object processcsv {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("processcsv").getOrCreate()

    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val csvdata = sc.parallelize(Array(
      "col_1, col_2, col_3",
      "1, ABC, Foo1",
      "2, ABCD, Foo2",
      "3, ABCDE, Foo3",
      "4, ABCDEF, Foo4",
      "5, DEF, Foo5",
      "6, DEFGHI, Foo6",
      "7, GHI, Foo7",
      "8, GHIJKL, Foo8",
      "9, JKLMNO, Foo9",
      "10, MNO, Foo10"))

    import sparkSession.implicits._

    val csvrdd = csvdata.map(line => line.split(",")).
      filter(lines => lines.length == 3 && lines(0) != "col_1").
      map(row => (row(0), row(1), row(2)))

    val csvdframe = csvrdd.toDF("col_1", "col_2", "col_3")
    csvdframe.show

    def dfSchema(columnNames: List[String]): StructType =
      StructType(
        Seq(
          StructField(name = "col_1", dataType = StringType, nullable = false),
          StructField(name = "col_2", dataType = StringType, nullable = false),
          StructField(name = "col_3", dataType = StringType, nullable = false)))

    def row(line: (String, String, String)): Row = Row(line._1, line._2, line._3)

    val schema = dfSchema(List("col_1", "col_2", "col_3"))

    val data = csvrdd.map(row)
    
    val dataFrame = sparkSession.createDataFrame(data, schema)

    dataFrame.show
    
    val kvdata = sc.parallelize(Array(
      "Row-Key-001, K1, 10, A2, 20, K3, 30, B4, 42, K5, 19, C20, 20",
      "Row-Key-002, X1, 20, Y6, 10, Z15, 35, X16, 42",
      "Row-Key-003, L4, 30, M10, 5, N12, 38, O14, 41, P13, 8"))
      
     val kvrdd = kvdata.map(line => line.split(",")).map(row => 
       {
         val rowList = row.toList.tail.zipWithIndex
         val alist = rowList.map(x => if (x._2 %2  == 0) (row(0), x._1))
         alist.filter( x=> x != () )
       }
    )
     kvrdd.foreach(println)
    
    sc.stop
  }
}