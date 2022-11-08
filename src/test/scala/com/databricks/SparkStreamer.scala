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
}
 */
