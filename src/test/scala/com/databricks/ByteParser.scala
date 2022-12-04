package com.databricks.labs.sparkstreaming.jsonmrf

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{InputStreamReader, BufferedInputStream}
import java.util.zip.GZIPInputStream


class ByteParserTest extends StreamingTest with BeforeAndAfter{

  test("parseUntilArrayLeft()"){
    assert ( bytesRead == 711 )
    assert ( ByteParser.parseUntilArrayLeft(buffer, bytesRead, 0) == 75 )
    assert ( buffer(75).toChar == '[' )
  }

  test("getQuotedStringRight()"){
    assert ( bytesRead == 711 )
    assert ( ByteParser.getQuotedStringRight(buffer, 72, bytesRead) == Some("provider_references") )
    assert ( ByteParser.getQuotedStringRight(buffer, 434, bytesRead) == Some("in_network") )
  }

  test("arrayHasNext()"){
    assert ( bytesRead == 711 )
    assert ( ByteParser.arrayHasNext(buffer, 227, bytesRead) == 231 )
  }

  test("findByteRight() / findByteLeft()"){

    //LEFT
    assert ( bytesRead == 711 )
    assert ( ByteParser.findByteRight(buffer, ByteParser.Colon, 75, bytesRead) ==  73  && buffer(73).toChar == ByteParser.Colon.toChar )
    assert ( ByteParser.findByteRight(buffer, ByteParser.Quote, 75, bytesRead) ==  72  && buffer(72).toChar == ByteParser.Quote.toChar )
    assert ( ByteParser.findByteRight(buffer, '{'.toInt, 75, bytesRead) == -1 ) // this is -1 because we cannot confirm if it is escaped or not
    assert ( ByteParser.findByteRight(buffer, ','.toInt, 75, bytesRead) == 46 && buffer(46).toChar == ',' )

    //RIGHT
    assert ( ByteParser.findByteLeft(buffer, '{'.toInt, 75, bytesRead) == 78 && buffer(78).toChar == '{' )
    assert ( ByteParser.findByteLeft(buffer, '}'.toInt, 75, bytesRead) == 217 && buffer(217).toChar == '}' )
    assert ( ByteParser.findByteLeft(buffer, ']'.toInt, 75, bytesRead) == 218 && buffer(218).toChar == ']' )
  }

  test("skipWhiteSpaceRight() / skipWhiteSpaceLeft()"){
    assert ( bytesRead == 711 )
    assert ( ByteParser.skipWhiteSpaceLeft(buffer, 229, bytesRead) == 231 )
    assert ( ByteParser.skipWhiteSpaceRight(buffer, 5, bytesRead) == 0 )
  }

  test("findByteArrayEndingLeft()"){
    assert ( ByteParser.findByteArrayEndingLeft(buffer, 200, bytesRead) == 217 )
    assert ( ByteParser.findByteArrayEndingLeft(buffer, 218, bytesRead) == 218 ) 
  }

  test("seekEndOfArray()"){
    assert ( ByteParser.seekEndOfArray(buffer, 78, bytesRead) == (380, 386) ) //(location of last element, location of closing brace)
    assert ( buffer(380) == '}'.toByte && buffer(386) == ']'.toByte )
    assert ( ByteParser.seekEndOfArray(buffer, 231, bytesRead) == (380, 386) )

  }

  test("skipWhiteSpaceAndComma()"){
    assert( bytesRead == 711 )
    ByteParser.skipWhiteSpaceAndCommaLeft(buffer, 228, bytesRead)
  }

  test("seekMatchingEndBracket()"){
    assert( bytesRead == 711 )
    assert( ByteParser.seekMatchingEndBracket(buffer, 78, bytesRead) == 227 )
    assert( buffer(227).toChar == '}' )
  }
}

/*
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
 .trigger(availableNow=true)
 .outputMode("append")
 .option("truncate", "false")
 .format("console")
 .start


}




 import com.databricks.labs.sparkstreaming.jsonmrf._
 import org.apache.spark.sql.SparkSession
 val spark = SparkSession.builder().master("local").config("spark.driver.memory", "8G").config("spark.driver.cores", 2).config("spark.executor.instances", 2).appName("Spark Streaming Example").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
val df = spark.readStream.format("com.databricks.labs.sparkstreaming.jsonmrf.JsonMRFSourceProvider").load("/Users/aaron.zavora//Downloads/umr-tpa-encore-in-network-rates.json")

df.writeStream
    .outputMode("append")
    .format("text")
    .queryName("movies")
    .option("checkpointLocation", "src/test/resources/chkpoint_dir")
    .start("src/test/resources/output")
 */
