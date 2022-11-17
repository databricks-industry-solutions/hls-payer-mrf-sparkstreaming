
// TODO add scala test package to build.sbt
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
//val fileStream = fs.open(new Path("src/test/resources/test.json"))
val BufferSize = 268435456 //256MB
val inStream = new BufferedInputStream(fileStream, BufferSize) 
//inStream.available == 2GB == 2147483647

val buffer = new Array[Byte](BufferSize)
val bytesRead = inStream.read(buffer,0, BufferSize)

//Parse Header
val arrayStartIndex = ByteParser.parseUntilArrayLeft(buffer, bytesRead)
val arrayKeyTuple = ByteParser.searchKeyRight(buffer, bytesRead, arrayStartIndex) //returns array key and the beginning location of the key
val headerEnding = ByteParser.findByteRight(buffer, ByteParser.Comma, arrayKeyTuple._2, bytesRead)
var header = buffer.slice(0, headerEnding + 1)
header(headerEnding) = ByteParser.CloseB.toByte

//Parse provider_references...
var startIndex = ByteParser.findByteLeft(buffer, ByteParser.OpenB, headerEnding+1, bytesRead)
var validCheckpoint = -1
var endIndex = -1
var prevChunk = Seq[Array[Byte]]()

if ( arrayKeyTuple._1  == "provider_references" ) { //iterate through to the end of this array

  ByteParser.seekEndOfArray(buffer, startIndex, bytesRead)


  while (ByteParser.endOfArray(buffer,arrayElementEnd,bytesRead) == -1 )  {
    arrayElementStart =  ByteParser.findByteLeft(buffer, ByteParser.OpenB, arrayElementEnd, bytesRead)
    arrayElementEnd = ByteParser.seekMatchingEndBracket(buffer, arrayElementStart, bytesRead)
    if (arrayElementEnd != -1) lastValidEnd = arrayElementEnd
  }

  //buffer.slice(250872936, 268297444).map(_.toChar)

}
var arrayElementStart = ByteParser.findByteLeft(buffer, ByteParser.OpenB, headerEnding+1, bytesRead)
var arrayElementEnd = ByteParser.seekMatchingEndBracket(buffer, arrayElementStart, bytesRead)
var endArray = -1

  
}

var nextArrayElementStart = ByteParser.seekNextArrayElementLeft(buffer, arrayElementStart, bytesRead)






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




