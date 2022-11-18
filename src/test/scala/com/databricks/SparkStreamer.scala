import com.databricks.labs.sparkstreaming.jsonmrf._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{InputStreamReader, BufferedInputStream}
import java.util.zip.GZIPInputStream
 
val spark = SparkSession.builder().master("local[2]").config("spark.driver.memory", "4G").appName("Spark Streaming Example").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
val hadoopConf = spark.sqlContext.sparkSession.sessionState.newHadoopConf()
val fs = FileSystem.get(hadoopConf)

val BufferSize = 268435456 //256MB
//val fileStream = fs.open(new Path("src/test/resources/test.json"))
//val fileStream = fs.open(new Path("/Users/aaron.zavora/Downloads/2022-10-05_a7f8868c-12c4-412c-b93b-29288e276377_Aetna-Life-Insurance-Company.json.gz"), BufferSize)
val fileStream = new GZIPInputStream(fs.open(new Path("/Users/aaron.zavora/Downloads/umr-tpa-encore-in-network-rates.json.gz"), 8192))
val inStream = new BufferedInputStream(fileStream, BufferSize)

val buffer = new Array[Byte](BufferSize)
val bytesRead = inStream.read(buffer,0, BufferSize)

val startIndex = 78
val arrLength = 711
val arr = buffer

ByteParser.seekEndOfArray(arr, 78, arrLength)

import com.databricks.labs.sparkstreaming.jsonmrf._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{InputStreamReader, BufferedInputStream}
import java.util.zip.GZIPInputStream
val spark = SparkSession.builder().master("local[2]").config("spark.driver.memory", "4G").appName("Spark Streaming Example").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("src/test/resources/test.json")
  df.writeStream
    .outputMode("append")
    .format("text")
    .queryName("movies")
    .option("checkpointLocation", "src/test/resources/chkpoint_dir")
    .start("src/test/resources/output")




test("spark streaming on larger dataset"){
import com.databricks.labs.sparkstreaming.jsonmrf._
import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder().master("local[2]").config("spark.driver.memory", "4G").appName("Spark Streaming Example").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("/Users/aaron.zavora/python/payer-pricing/data/2022-10-05-aetna-life-insurance.json")
  df.writeStream
    .outputMode("append")
    .format("text")
    .queryName("movies")
    .option("checkpointLocation", "src/test/resources/chkpoint_dir")
    .start("src/test/resources/output")
    .awaitTermination





  import com.databricks.labs.sparkstreaming.jsonmrf._
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().master("local[2]").config("spark.driver.memory", "4G").appName("Spark Streaming Example").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("src/test/resources/test.json")

  df.writeStream
    .outputMode("append")
    .option("truncate", "false")
    .format("console")
    .start
    .awaitTermination


}




import com.databricks.labs.sparkstreaming.jsonmrf._
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().master("local[2]").config("spark.driver.memory", "4G").appName("Spark Streaming Example").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("/Users/aaron.zavora/Downloads/umr-tpa-encore-in-network-rates.json.gz")
//val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("/Users/aaron.zavora//Downloads/umr-tpa-encore-in-network-rates.json")
//val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("/Users/aaron.zavora/Downloads/2022-10-05_a7f8868c-12c4-412c-b93b-29288e276377_Aetna-Life-Insurance-Company.json.gz")

df.writeStream
    .outputMode("append")
    .format("text")
    .queryName("movies")
    .option("checkpointLocation", "src/test/resources/chkpoint_dir")
    .start("src/test/resources/output")
