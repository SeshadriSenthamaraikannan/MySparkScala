package com.toyota.pqssurvey

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import java.text.SimpleDateFormat
import java.util.Calendar

object Practice {
  
  def main(args: Array[String]){
    
    val List1 = List.range(1,10);
    
    println(sum(List1));
    
    println(sum2(List1));
    
    def sum(Ints: List[Int]) : Int = Ints match {
      
      case Nil => 0
      case x :: tail => x + sum(tail)
      
      
    }
    
    // Using Tail Recursive Method
def sum2(ints: List[Int]): Int = {
   // @tailrec
    def sumAccumulator(ints: List[Int], accum: Int): Int = {
      ints match {
        case Nil => accum
        case x :: tail => sumAccumulator(tail, accum + x)
      }
    }
    sumAccumulator(ints, 0)
  }
    
    
var things = (1,"January");

}
  
  /*  val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.PqsFeatureComments")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.PQSConnectServices3")
      .getOrCreate()
      
      val NewList = List("Seshadri","Programmer","Chennai")
      
      val NewMap = sparkSession.sqlContext.sparkContext.parallelize(Seq(NewList))
    
  val NewRMap = NewMap.map(x => {
    
    println("First Value is" + x(0));
    
  })*/
      

      
    
  }
  
