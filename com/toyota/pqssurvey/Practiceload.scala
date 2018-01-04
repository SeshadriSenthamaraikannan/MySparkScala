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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer




object Practiceload {
  
  case class summa( Vin: String)
  
   
  def main(args: Array[String]) {
    //Initializing Spark Context

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.PqsFeatureComments")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.PqsFeatureCommentsOut")
      .getOrCreate()

  val rdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/Feature.txt")
  
  val zz = rdd.collect().toList
  
  println(zz)
/*
  val xx = rdd.map(x=> Row(x(0)) ).toString()
 

  
 summa(xx)
  
   rdd.saveAsTextFile("file:///C:/Users/402960/ccc3.txt")
    
 */   
    
  }
  
}