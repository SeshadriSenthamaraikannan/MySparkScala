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

object Response {
  
  case class all(a: String, b: String, c: String, d: String, e: String, f: String, g: String, h: String, i: String, j: String, k: String, l: String, m: String, n: String, o: String, p: String, q: String, r: String, s: String, t: String, u: String, v: String, w: String, x: String, y: String, z: String, aa: String, bb: String, cc: String, dd: String, ee: String, ff: String, gg: String, hh: String, ii: String, jj: String, kk: String, ll: String, mm: String, nn: String, oo: String)

  case class surveyResponse(Problem_Category: String, Problem_Sub_Category: String, Question_Response: String, Comments: String, Vehicle_Satisfaction: String, Value_Satisfaction: String, Dealer_Satisfaction: String, Quality_Satisfaction: String, Likely_to_Service: String, Likely_to_Recommend: String, Likely_to_Purchase: String, New_To_Toyota_Family: String, QuesNbr: String)
  case class customer(Height: String, Weight: String, Riders: String, Gender: String, Age: String, Cell_Phone: String, Cell_Provider: String,Customer_State: String, Customer_Zip: String )
  case class vehicle(Vin: String, Model_number: String, Model_Year: String, Model_Name: String,Report_Month: String, Plant: String, Engine: String, Line_off_date: String, Body_style: String, Wave: String, Sale_Month: String, Region: String, Tires: String, Audio: String, Mileage: String,Report_Week: String, Survey_Version: String, Katashiki: String)
  case class audit(User: String, Create_ts: String, Update_ts: String)
  case class Survey(_id: String, SurveyID: String, SurveyType: String, Customer: customer, Vehicle: vehicle, SurveyResponse: Array[surveyResponse], Audit: audit)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.PqsResponse")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.PqsResponseOut")
      .getOrCreate()

    //    val dd = sparkSession.read.option("header", "true").csv("file:///C:/Users/130486/Downloads/transpose.txt")
    //    val df = sparkSession.table("eidh_stage.stg_pqs2comments")
    val rdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_Response.txt")
    val xx = rdd.map(x => x.split("\\|"))

    val zz = xx.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40)))

    import sparkSession.implicits._

    val rddPaired = zz.map(x => (x(36).toString(), x))

    val rddSurvey = rddPaired.groupByKey().map(x => {

      println("X_1......." + x._1)

      val SurveyID = x._1.trim()

      val list1 = x._2.toArray

      val SurveyType = "PQS Response"
      val _id = x._1.trim() + "_" + SurveyType
      val Vin = list1(0)(0).toString.trim()
      val Model_Number = list1(0)(1).toString.trim()
      val Model_Year = list1(0)(2).toString.trim()
      val Model_Name = list1(0)(3).toString.trim()
      val Report_Month = list1(0)(7).toString.trim()
      val Plant = list1(0)(8).toString.trim()
      val Engine = list1(0)(9).toString.trim()
      val Line_Off_Date = list1(0)(10).toString.trim()
      val Body_Style = list1(0)(11).toString.trim()
      val Wave = list1(0)(12).toString.trim()
      val Sale_Month = list1(0)(13).toString.trim()
      val Region = list1(0)(14).toString.trim()
      val Tires = list1(0)(15).toString.trim()
      val Audio = list1(0)(16).toString.trim()
      val Mileage = list1(0)(17).toString.trim()
      val Height = list1(0)(18).toString.trim()
      val Weight = list1(0)(19).toString.trim()
      val Rides = list1(0)(20).toString.trim()
      val Gender = list1(0)(21).toString.trim()
      val Age = list1(0)(22).toString.trim()
      val Survey_Version = list1(0)(33).toString.trim()
      val Katashiki = list1(0)(40).toString.trim()
      val Report_Week = list1(0)(24).toString.trim()
      val Cell_Phone = list1(0)(34).toString.trim()
	  val Cell_Provider = list1(0)(35).toString.trim()
	  val Customer_State = list1(0)(38).toString.trim()
	  val Customer_Zip = list1(0)(39).toString.trim()
	  
	  
      val responseArr = x._2.toArray.map(x => {

        surveyResponse(x(4).toString().trim(), x(5).toString().trim(), x(6).toString().trim(), x(23).toString().trim(), x(25).toString().trim(), x(26).toString().trim(), x(27).toString().trim(), x(28).toString().trim(), x(29).toString().trim(), x(30).toString().trim(), x(31).toString().trim(), 
		x(32).toString().trim(), x(37).toString().trim() )

      })

      val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts

      Survey(_id, SurveyID, SurveyType, customer(Height, Weight, Rides, Gender, Age, Cell_Phone, Cell_Provider,Customer_State,Customer_Zip), vehicle(Vin, Model_Number, Model_Year, Model_Name, Report_Month, Plant, Engine, Line_Off_Date, Body_Style, Wave, Sale_Month, Region, Tires, Audio, Mileage, Report_Week,Survey_Version, Katashiki), responseArr.toArray, audit(User, Create_ts, Update_ts))

    })

    val dfSurvey = rddSurvey.toDF()

    dfSurvey.printSchema()
    dfSurvey.show
    dfSurvey.write.format("com.mongodb.spark.sql").mode("append").save()

  }
  
  
}