package com.toyota.SurveyCollection

import org.apache.spark.sql._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.util
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, SparkSession }


class pqssurvey extends Comments{

 
override def process(sparkSession:SparkSession):(DataFrame) = {
  import sparkSession.implicits._
  val rdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_Comments.txt")
    val xx = rdd.map(x => x.split("\\|"))
    
    

    val zz = xx.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(35), x(36), x(37), x(38), x(39), x(40), x(41), x(42), x(43), x(44), x(45), x(46), x(47), x(48)))

    import sparkSession.implicits._

    val rddPaired = zz.map(x => (x(46).toString(), x))
    
    val rddSurvey = rddPaired.groupByKey()
    
   val a= rddSurvey.map{x => 

      println("X_1......." + x._1)

      val SurveyID = x._1.trim()

      val list1 = x._2.toArray

      val SurveyType = "pqs2comments"
      val _id = x._1.trim() + "_" + SurveyType
      val Vin = list1(0)(0).toString.trim()
      val Model_Number = list1(0)(1).toString.trim()
      val Model_Year = list1(0)(2).toString.trim()
      val Model_Name = list1(0)(3).toString.trim()
      val Report_Month = list1(0)(8).toString.trim()
      val Plant = list1(0)(9).toString.trim()
      val Engine = list1(0)(10).toString.trim()
      val Line_Off_Date = list1(0)(11).toString.trim()
      val Code2 = list1(0)(13).toString.trim()
      val Code3 = list1(0)(14).toString.trim()
      val Code4 = list1(0)(15).toString.trim()
      val Code5 = list1(0)(16).toString.trim()
      val Code6 = list1(0)(17).toString.trim()
      val Code7 = list1(0)(18).toString.trim()
      val Body_Style = list1(0)(19).toString.trim()
      val Count = list1(0)(20).toString.trim()
      val Wave = list1(0)(21).toString.trim()
      val Sale_Month = list1(0)(22).toString.trim()
      val Region = list1(0)(23).toString.trim()
      val District = list1(0)(24).toString.trim()
      val Dealer = list1(0)(25).toString.trim()
      val RDRDate = list1(0)(26).toString.trim()
      val ProdYRMM = list1(0)(27).toString.trim()
      val Tires = list1(0)(30).toString.trim()
      val Audio_Unit = list1(0)(31).toString.trim()
      val Mileage = list1(0)(32).toString.trim()
      val Height = list1(0)(33).toString.trim()
      val Weight = list1(0)(34).toString.trim()
      val Rides = list1(0)(35).toString.trim()
      val Gender = list1(0)(36).toString.trim()

      val Age = list1(0)(37).toString.trim()
      val Survey_Version = list1(0)(38).toString.trim()
      val Katashiki = list1(0)(39).toString.trim()
      val Report_Week = list1(0)(40).toString.trim()
      val Exterior_Color = list1(0)(45).toString.trim()
      val Interior_Color = list1(0)(46).toString.trim()

      val responseArr = x._2.toArray.map(x => {

        surveyResponse(x(4).toString().trim(), x(5).toString().trim(), x(6).toString().trim(), x(7).toString().trim(), x(12).toString().trim(), x(28).toString().trim(), x(29).toString().trim(), x(41).toString().trim(), x(42).toString().trim(), x(43).toString().trim(), x(44).toString().trim())

      })

      val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts

      Survey(_id, SurveyID, SurveyType, customer(Height, Weight, Rides, Gender, Age), vehicle(Vin, Model_Number, Model_Year, Model_Name, Report_Month, Plant, Engine, Line_Off_Date, Code2, Code3, Code4, Code5, Code6, Code7, Body_Style, Count, Wave, Sale_Month, Region, District, Dealer, RDRDate, ProdYRMM, Tires, Audio_Unit, Mileage, Survey_Version, Katashiki, Report_Week, Exterior_Color, Interior_Color), responseArr.toArray, audit(User, Create_ts, Update_ts))
     
        }
  val dfSurvey = rddSurvey.toDF()

      dfSurvey.printSchema()
      dfSurvey.show
      //dfSurvey.write.format("com.mongodb.spark.sql").mode("append").save()
  
  dfSurvey
  }

   
  override def process1(sparkSession:SparkSession):(DataFrame) = {
    val rdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_FeatureComments.txt")
    val xx = rdd.map(x => x.split("\\|"))

    val zz = xx.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40), x(41), x(42), x(43)))

    import sparkSession.implicits._

    val rddPaired = zz.map(x => (x(42).toString(), x))
    

    val rddSurvey = rddPaired.groupByKey()
    val b =rddSurvey.map{x => 
      
      println("X_1......." + x._1)
      
      val SurveyID = x._1.trim()

      val list1 = x._2.toArray

      val SurveyType = "pqs2featurecomments"
      val _id = x._1.trim() + "_" + SurveyType
      val Vin = list1(0)(0).toString.trim()
      val Model_Number = list1(0)(1).toString.trim()
      val Model_Year = list1(0)(2).toString.trim()
      val Model_Name = list1(0)(3).toString.trim()
      val Report_Month = list1(0)(8).toString.trim()
      val Plant = list1(0)(9).toString.trim()
      val Engine = list1(0)(10).toString.trim()
      val Line_Off_Date = list1(0)(11).toString.trim()
      val Code2 = list1(0)(13).toString.trim()
      val Code3 = list1(0)(14).toString.trim()
      val Code4 = list1(0)(15).toString.trim()
      val Code5 = list1(0)(16).toString.trim()
      val Code6 = list1(0)(17).toString.trim()
      val Code7 = list1(0)(18).toString.trim()
      val Body_Style = list1(0)(19).toString.trim()
      val Count = list1(0)(20).toString.trim()
      val Wave = list1(0)(21).toString.trim()
      val Sale_Month = list1(0)(22).toString.trim()
      val Region = list1(0)(23).toString.trim()
      val District = list1(0)(24).toString.trim()
      val Dealer = list1(0)(25).toString.trim()
      val RDRDate = list1(0)(26).toString.trim()
      val ProdYRMM = list1(0)(27).toString.trim()
      val Tires = list1(0)(30).toString.trim()
      val Audio_Unit = list1(0)(31).toString.trim()
      val Mileage = list1(0)(32).toString.trim()
      val Height = list1(0)(33).toString.trim()
      val Weight = list1(0)(34).toString.trim()
      val Rides = list1(0)(35).toString.trim()
      val Gender = list1(0)(36).toString.trim()
      val Age = list1(0)(37).toString.trim()
      val Survey_Version = list1(0)(38).toString.trim()
      val Katashiki = list1(0)(39).toString.trim()
      val Report_Week = list1(0)(40).toString.trim()
      val responseArr = x._2.toArray.map(x => {

        surveyResponse_feature(x(4).toString().trim(), x(5).toString().trim(), x(6).toString().trim(), x(7).toString().trim(), x(12).toString().trim(), x(28).toString().trim(), x(29).toString().trim(), x(41).toString().trim(), x(43).toString().trim())

      })

      val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts

      Survey_feature(_id, SurveyID, SurveyType, customer(Height, Weight, Rides, Gender, Age), vehicle_feature(Vin, Model_Number, Model_Year, Model_Name, Report_Month, Plant, Engine, Line_Off_Date, Code2, Code3, Code4, Code5, Code6, Code7, Body_Style, Count, Wave, Sale_Month, Region, District, Dealer, RDRDate, ProdYRMM, Tires, Audio_Unit, Mileage, Survey_Version, Katashiki, Report_Week), responseArr.toArray, audit(User, Create_ts, Update_ts))
         }
    val dfSurvey = rddSurvey.toDF()

    dfSurvey.printSchema()
    dfSurvey.show
    //dfSurvey.write.format("com.mongodb.spark.sql").mode("append").save()
    
    dfSurvey
  }


override def process3(sparkSession:SparkSession)={
  
  
    val rdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_Response.txt")
    val xx = rdd.map(x => x.split("\\|"))

    val zz = xx.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40)))

    import sparkSession.implicits._

    val rddPaired = zz.map(x => (x(36).toString(), x))

    val rddSurvey = rddPaired.groupByKey()
    
    val c= rddSurvey.map{x => 
    
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

        surveyResponse_pqsresp(x(4).toString().trim(), x(5).toString().trim(), x(6).toString().trim(), x(23).toString().trim(), x(25).toString().trim(), x(26).toString().trim(), x(27).toString().trim(), x(28).toString().trim(), x(29).toString().trim(), x(30).toString().trim(), x(31).toString().trim(), 
		x(32).toString().trim(), x(37).toString().trim() )

      })

      val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts

      Survey_pqsresp(_id, SurveyID, SurveyType, customer_pqsresp(Height, Weight, Rides, Gender, Age, Cell_Phone, Cell_Provider,Customer_State,Customer_Zip), vehicle_pqsresp(Vin, Model_Number, Model_Year, Model_Name, Report_Month, Plant, Engine, Line_Off_Date, Body_Style, Wave, Sale_Month, Region, Tires, Audio, Mileage, Report_Week,Survey_Version, Katashiki), responseArr.toArray, audit_pqsresp(User, Create_ts, Update_ts))

    }
    val dfSurvey = rddSurvey.toDF()

    dfSurvey.printSchema()
    dfSurvey.show
    //dfSurvey.write.format("com.mongodb.spark.sql").mode("append").save()
    dfSurvey
  }
}


