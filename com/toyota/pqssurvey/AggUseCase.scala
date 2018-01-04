package com.toyota.pqssurvey

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

object AggUseCase {
  
    def main(args: Array[String]) {

   
      val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
     .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.FinalCollectionSurveyV7")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.ModelYearData")
   /*   .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsin")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsout1")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqsresponse")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqsresponseout")*/
      .getOrCreate()
      
 import com.mongodb.spark.config._  
 
 println("Reading from the 'FinalCollectionSurveyV7' collection:")
val Final_Survey_Collection = MongoSpark.load(sparkSession, ReadConfig(Map("collection" -> "FinalCollectionSurveyV7"), Some(ReadConfig(sparkSession))))//.show()
      
 
 Final_Survey_Collection.filter("SurveyID=1612310038030").show()
 
val toInt = udf[ Int, String] (_.toInt)

val New_df = Final_Survey_Collection.withColumn("NEWID",toInt(Final_Survey_Collection("SurveyID")))
    
   New_df.groupBy("SurveyID").max("NEWID").show()
   New_df.groupBy("SurveyID").min("NEWID").show()

     
//val teenagers = sparkSession.sql("SELECT * FROM Final_Survey_Collection_Renamed WHERE SurveyID =1611190046673").show() 
    
    }
   
}