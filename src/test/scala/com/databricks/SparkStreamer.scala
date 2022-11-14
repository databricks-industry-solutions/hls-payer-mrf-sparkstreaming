/*
 TODO add scala test package to build.sbt
import com.databricks.labs.sparkstreaming.jsonmrf._
 import org.apache.spark.sql.SparkSession

class testFunctionsSuite 
{
  test("testing streaming package..."){
    val spark = SparkSession.builder().master("local[2]").appName("Spark Streaming Example").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()
    val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("src/test/resources/test.json")
    df.writeStream.format("console").start()
    assert(df.isStreaming, true)
 }

 test("test streaming on gz file..."){
 val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("src/test/resources/test.json.gz")
 df.writeStream
 .outputMode("append")
 .option("truncate", "false")
 .format("console")
 .start
 .awaitTermination
 }

 test("test on a larger dataset..."){
 

 }
}

import com.databricks.labs.sparkstreaming.jsonmrf._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
 import java.io.{InputStreamReader, BufferedInputStream}
 
val spark = SparkSession.builder().master("local[2]").config("spark.driver.memory", "4G").appName("Spark Streaming Example").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
val hadoopConf = spark.sqlContext.sparkSession.sessionState.newHadoopConf()
val fs = FileSystem.get(hadoopConf) 
 val fileStream = fs.open(new Path("/Users/aaron.zavora/python/payer-pricing/data/2022-10-05-aetna-life-insurance.json"))
 val BufferSize = 2147483647
val inStream = new BufferedInputStream(fileStream, BufferSize) //256MB buffer 


 */
