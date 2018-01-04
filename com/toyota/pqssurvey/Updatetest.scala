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

object Updatetest {
  
  case class all(a: String, b: String, c: String, d: String, e: String, f: String, g: String, h: String, i: String, j: String, k: String, l: String, m: String, n: String, o: String, p: String, q: String, r: String, s: String, t: String, u: String, v: String, w: String, x: String, y: String, z: String, aa: String, bb: String, cc: String, dd: String, ee: String, ff: String, gg: String, hh: String, ii: String, jj: String, kk: String, ll: String, mm: String, nn: String, oo: String, pp: String, qq: String, rr: String)

  case class surveyResponse(Problem_Category: String, Category_Name: String, Problem_Sub_Category: String, Sub_Category_Name: String, Code1: String, Comment: String, Condition: String, New_To_Toyota_Family: String, QuesNbr: String)
  case class customer(Height: String, Weight: String, Riders: String, Gender: String, Age: String)
  case class vehicle(Vin: String, Model_number: String, Model_Year: String, Model_Name: String, Report_month: String, Plant: String, Engine: String, Line_off_date: String, Code2: String, Code3: String, Code4: String, Code5: String, Code6: String, Code7: String, Body_style: String, Count: String, Wave: String, Sale_Month: String, Region: String, District: String, Dealer: String, RDRDate: String, ProdYRMM: String, Tires: String, Audio_Unit: String, Mileage: String, Survey_Version: String, Katashiki: String, Report_Week: String)
  case class audit(User: String, Create_ts: String, Update_ts: String)
  case class Survey(_id: String, SurveyID: String, SurveyType: String, Customer: customer, Vehicle: vehicle, SurveyResponse: Array[surveyResponse], Audit: audit)
  case class NEW(_id: String, SurveyID: String, SurveyType: String,Vin: String)
  
  
  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.PqsFeatureComments")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.PqsFeatureCommentsOut")
      .getOrCreate()

  val rdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/Feature.txt")
    val xx = rdd.map(x => x.split("\\|"))

    val zz = xx.map(x =>

      Row(x(0), x(1)))

    import sparkSession.implicits._

    val rddPaired = zz.map(x => (x(1).toString(), x))
    

    val rddSurvey = rddPaired.groupByKey().map(x => {

      println("X_1......." + x._1)
      
      
      val SurveyID = x._1.trim()

      val list1 = x._2.toArray

      val SurveyType = "pqs2featurecomments"
      val _id = x._1.trim() + "_" + SurveyType
      val Vin = list1(0)(0).toString.trim()
     
      NEW(_id,SurveyID,SurveyType,Vin)
      
      
    })

    val dfSurvey = rddSurvey.toDF()

    dfSurvey.printSchema()
    dfSurvey.show
    dfSurvey.write.format("com.mongodb.spark.sql").mode("append").option("replaceDocument", "false").save()

  }
  
  
}