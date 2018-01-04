package helloworld


import org.apache.spark.sql._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.util
import org.apache.spark.sql.functions.{concat, lit}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object MyDataset {
  
  def main(args: Array[String]){
    
              val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.testCollection1")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.JoinedCollection")
    .getOrCreate()
    
    case class Mydata(Name:String,City:String,State:String,Religion:String)
    
    case class Person(Name:String, Age:Int)
    
 //   val mydataset = sparkSession.readStream.csv(path)
    
    val Mydataset = sparkSession.sparkContext.textFile("file:///C:/Streaming_Data/Mydata.txt")
    
    val myrdd = Mydataset.map(x=>x.split(","))
    
    val myrddrow = myrdd.map{x=>
                Row(x(0),x(1),x(2),x(3))
                var Name = x(0).toString();
                var City = x(1).toString();
                var State =x(2).toString();
                var Religion = x(3).toString();
                Mydata(Name,City,State,Religion)
              }
                 
   import sparkSession.implicits._
 //var mydf = myrddrow.toDF()
 //val myds = myrddrow.toDS()
 
 

  }
  
}