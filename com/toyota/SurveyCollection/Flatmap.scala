/*package com.toyota.SurveyCollection

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
import com.mongodb.spark.MongoSpark 

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object Flatmap {
  
  def main(args: Array[String]){
    
     val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.testCollection1")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.FinalCollectionSurveyV7")
   /*   .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsin")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsout1")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqsresponse")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqsresponseout")*/
      .getOrCreate()
      
       val TextFile = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_Response.txt")
      
  //    val TextFile1 = Seq("India","country")
      
    //  val TextFile = sparkSession.sparkContext.parallelize(TextFile1)
        
        val new_xx = TextFile.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_).collect
        
        val new_char = TextFile.flatMap(line => line.split(" ")).map(char => (char,1)).reduceByKey(_+_).collect
        
     //   new_char.foreach(println)
        
       val new_val =  TextFile.map(_.length).sum()
       
       println("output is" + new_val)
 /**       
        for ( a <- new_xx){
          
          println("output is" + a)
          
        }
     
     for ((k,v )<-new_xx if v >10){
       
       println(s"Value is + $v")
       
     }
        
        val new_yy = new_xx.filter(x => x._1 != null )
        
    new_xx.foreach(println)    
    
           
    
  
 
  */
  
   val s: String = "22/08/2013"   
   
   val Format:SimpleDateFormat = 
   
       
  } 
}*/