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


object SparkStreaming {
  
  def main(args: Array[String]){
    
     val sparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.testCollection1")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.NewStreamCollection")
    .getOrCreate()
    
    println("Spark Streaming Using Text file")
    
  //  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    
    val ssc = new StreamingContext(sparkSession.sparkContext,Seconds(15))
    
    
  /*  val linesDataStream = ssc.textFileStream("C:/Streaming_Data")
    
    val words = linesDataStream.flatMap(_.split(" "))
    
    val pairs = words.map(word => (word,1))
    
    val count = pairs.reduceByKey(_+_)
    
    count.print();*/
    
   // val df = sparkSession.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","exampletopic")
   //          .option("startingOffsets", "earliest").load()
             
   val lines = KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-group",Map("exampletopic" -> 5))
    
   lines.print();
    
    case class myname(Name:String)
    
    val schema = StructType(
    StructField("k", StringType, true)
     :: Nil)
  
var mylines = lines.map(_._2)



 mylines.foreachRDD{rdd=>
  
 
  val sqlcontext = SQLContext.getOrCreate(rdd.sparkContext)
 
  import sqlcontext.implicits._
  val mydf = rdd.map{row=>
    Row(row)
   }
  import sqlcontext.implicits._
  val mydataf = sparkSession.createDataFrame(mydf, schema)
  mydataf.show()
 mydataf.write.format("com.mongodb.spark.sql").mode("append").option("replaceDocument", "false").save()
}
  //  Final_Survey_Collection_Renamed.write.format("com.mongodb.spark.sql").mode("append").option("replaceDocument", "false").save() 
 
    ssc.start()            
    ssc.awaitTermination() 
    
  }
  
}