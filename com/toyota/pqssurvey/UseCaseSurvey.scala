/*package com.toyota.pqssurvey

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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

object UseCaseSurvey {
  
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
val characters = MongoSpark.load(sparkSession, ReadConfig(Map("collection" -> "FinalCollectionSurveyV7"), Some(ReadConfig(sparkSession))))//.show()


  val Structure = 
    StructType(
        StructField("QuesCode",StringType, true) ::
        StructField("category_code",StringType,true) ::
        StructField("sub_category_code", StringType, true) ::
        Nil)

case class Company(name: String, foundingYear: Int, numEmployees: Int)
val inputSeq = List('a','b','c')
val df = sparkSession.sqlContext.sparkContext.parallelize(inputSeq)

val datadf = sparkSession.createDataFrame(df, Structure);

val companyDS = df.as[Company]
companyDS.show()


characters.createOrReplaceTempView("Character")

val Response = sparkSession.sql("select * from Character where substr(Vehicle.Model_Year,1,4) = 2017 and Customer.Gender='Female'");
      
            
Response.show(); 

Response.printSchema();

Response.write.format("com.mongodb.spark.sql").mode("append").option("replaceDocument", "false").save()
    
      
  }
  
}*/