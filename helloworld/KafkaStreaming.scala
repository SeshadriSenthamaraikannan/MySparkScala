package helloworld

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel



object KafkaStreaming {
  
    def main(args: Array[String]){
      
         val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.testCollection1")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.JoinedCollection")
    .getOrCreate()
      
       //   val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming")
    
    val ssc = new StreamingContext(sparkSession.sparkContext,Seconds(2))
                    
     val KafkaParam = Map("metadata.broker.list" -> "localhost:9092");
     
          val topics = List("exampletopic").toSet
          
          val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,KafkaParam,topics).map(_._2)
          
          lines.print()
              
          
        ssc.start()            
    ssc.awaitTermination() 
 
      
    }
  
}