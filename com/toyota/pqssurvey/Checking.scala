package com.toyota.pqssurvey

import org.apache.spark.sql._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.ArrayType
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.util
import org.apache.spark.sql.SQLContext 
 

object Checking {
  
  case class surveyResponse_resp(Ques_code: String,Problem_Category: String, Problem_Sub_Category: String, Question_Response: String, Comments: String, Vehicle_Satisfaction: String, Value_Satisfaction: String, Dealer_Satisfaction: String, Quality_Satisfaction: String, Likely_to_Service: String, Likely_to_Recommend: String, Likely_to_Purchase: String, New_To_Toyota_Family: String, QuesNbr: String)
  case class customer_resp(Height: String, Weight: String, Riders: String, Gender: String, Age: String, Cell_Phone: String, Cell_Provider: String,Customer_State: String, Customer_Zip: String )
  case class vehicle_resp(Vin: String, Model_number: String, Model_Year: String, Model_Name: String,Report_Month: String, Plant: String, Engine: String, Line_off_date: String, Body_style: String, Wave: String, Sale_Month: String, Region: String, Tires: String, Audio: String, Mileage: String,Report_Week: String, Survey_Version: String, Katashiki: String)
  case class audit_resp(User: String, Create_ts: String, Update_ts: String)
  case class Survey_resp(_id: String, SurveyID_Resp: String, SurveyType: String, Customer_Resp: customer_resp, Vehicle_Resp: vehicle_resp, SurveyResponse: Array[surveyResponse_resp], Audit: audit_resp)
  case class Temptable(Problem_Category: String, Problem_Sub_Category: String)
  
  val Structure = 
    StructType(
        StructField("QuesCode",StringType, true) ::
        StructField("category_code",StringType,true) ::
        StructField("sub_category_code", StringType, true) ::
        StructField("category_name", StringType, true) ::
        StructField("Sub_Category_Name", StringType, true) ::
        StructField("Section", StringType, true) ::
        StructField("Response_Type", StringType, true) ::
        StructField("SurveyVersion", StringType, true) ::
        StructField("Division", StringType, true) ::
        StructField("TableLoc", StringType, true) ::
        StructField("PQSCode", StringType, true) ::
        Nil)
      
  
   def main(args: Array[String]) {

   
      val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.testCollection1")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.PQSQuestion")
   /*   .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsin")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsout1")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqsresponse")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqsresponseout")*/
      .getOrCreate()
  /*
   val rdd1 = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQSCodeSpecs.txt")
   
   val inter = rdd1.first();
   
   val rdd = rdd1.filter(row => row != inter)
   
    val xx = rdd.map(x => x.split("\\|"))

    val zz = xx.map(x =>

      Row(x(0).toString.trim(), x(1).toString.trim(), x(2).toString.trim(), x(3).toString.trim(), x(4).toString.trim(), x(5).toString.trim(), x(6).toString.trim(), x(7).toString.trim(), x(8).toString.trim(), x(9).toString.trim(), x(10).toString.trim()))

    import sparkSession.implicits._

    

    val dfSurvey = sparkSession.createDataFrame(zz, Structure);
      

    dfSurvey.printSchema()
    dfSurvey.show
   
    
   */
             val quesrdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQSCodeSpecs.txt")
   
       val quesfirst = quesrdd.first();
   
       val quesrdd1 = quesrdd.filter(row => row != quesfirst)
   
       val quesxx = quesrdd1.map(x => x.split("\\|"))

       val queszz = quesxx.map(x =>

       (x(1).toString.trim()+x(2).toString.trim()-> x(0).toString.trim())).collectAsMap

       println("variable map.." + queszz )
       
      // val Lookup_val = queszz.value("1909")
             
       val broadCastLookupMap = sparkSession.sparkContext.broadcast(queszz)
       
       println("Broadcast variable..." + broadCastLookupMap)
       
       val Lookup_val = broadCastLookupMap.value("1909")
       
       println("Output..." + Lookup_val )

    
    val rdd_resp = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_Response.txt")
    val xx_resp = rdd_resp.map(x => x.split("\\|"))

    val zz_resp = xx_resp.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40)))

    import sparkSession.implicits._

    val rddPaired_resp = zz_resp.map(x => (x(36).toString(), x))

    val rddSurvey_resp = rddPaired_resp.groupByKey().map(x => {

      println("X_1......." + x._1)
      
     // println("X_2..." + x._2)

      val SurveyID_Resp = x._1.trim()

      val list1 = x._2.toArray
      
      println("list1..." + list1)

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
         
           import sparkSession.implicits._
  /*      
     //  val x1=if(x(4)!=null && !x(4).toString().isEmpty())  x(4).toString().trim() else ""
     var TempArr = Temptable(list1(0)(4).toString.trim(), list1(0)(5).toString.trim())
     
 var s1=  sparkSession.sparkContext.parallelize(List(TempArr), 1)
     var s2 = s1.toDF()//.createOrReplaceTempView("TempTable")
     
  // var New_DF = dfSurvey.join(s2,dfSurvey("category_code") === s2("Problem_Category")).join(s2,dfSurvey("sub_category_code") === s2("Problem_Sub_Category")).select("QuesCode").withColumn("QuesDesc",lit("Description"))
     
     var New_DF = dfSurvey.join(s2,dfSurvey("category_code") === s2("Problem_Category")).join(s2,dfSurvey("sub_category_code") === s2("Problem_Sub_Category")).select("QuesCode").withColumn("QuesDesc",lit("Description"))
   
     var New_DF1 = New_DF.select("QuesCode").toString()
     
     var New_DF2 = New_DF.select("QuesDesc").toString() 
     
*/
         
         
      val responseArr_resp = x._2.toArray.map(x => {
        
       
       
 val a = broadCastLookupMap.value.getOrElse(x(4).toString().trim()+x(5).toString().trim(), "")
 
 println("Bradcast A is value...." + a )
     
    // val joinsdf = dfSurvey.join(dfSurvey_feature,dfSurvey("SurveyID_Comm") === dfSurvey_feature("SurveyID_Feat")).join(dfSurvey_resp,dfSurvey("SurveyID_Comm") === dfSurvey_resp("SurveyID_Resp"))
     
 // val New_Temp =  Temp.toDF();
    
  // NEW_QUES = New_Temp.join(question,"condition").select("Quesitoncode").withcolumn("QuestionDesc",case->9001:desion1)

        //      val a = broadCastLookupMap.value(x(4).toString().trim()+x(5).toString().trim())
              
          //    println("Quescode..." + a )
        
        surveyResponse_resp("Code",x(4).toString().trim(), x(5).toString().trim(), x(6).toString().trim(), x(23).toString().trim(), x(25).toString().trim(), x(26).toString().trim(), x(27).toString().trim(), x(28).toString().trim(), x(29).toString().trim(), x(30).toString().trim(), x(31).toString().trim(), 
              x(32).toString().trim(), x(37).toString().trim() )

      })

      val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts

      Survey_resp(_id, SurveyID_Resp, SurveyType, customer_resp(Height, Weight, Rides, Gender, Age, Cell_Phone, Cell_Provider,Customer_State,Customer_Zip), vehicle_resp(Vin, Model_Number, Model_Year, Model_Name, Report_Month, Plant, Engine, Line_Off_Date, Body_Style, Wave, Sale_Month, Region, Tires, Audio, Mileage, Report_Week,Survey_Version, Katashiki), responseArr_resp.toArray, audit_resp(User, Create_ts, Update_ts))

    })

    val dfSurvey_resp = rddSurvey_resp.toDF()
    
   

    dfSurvey_resp.printSchema()
    dfSurvey_resp.show
    
    
   // dfSurvey.write.format("com.mongodb.spark.sql").mode("append").save()
  
}
  
}  