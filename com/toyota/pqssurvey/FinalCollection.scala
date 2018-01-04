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
 

object FinalCollection {
  
  
  
  case class all(a: String, b: String, c: String, d: String, e: String, f: String, g: String, h: String, i: String, j: String, k: String, l: String, m: String, n: String, o: String, p: String, q: String, r: String, s: String, t: String, u: String, v: String, w: String, x: String, y: String, z: String, aa: String, bb: String, cc: String, dd: String, ee: String, ff: String, gg: String, hh: String, ii: String, jj: String, kk: String, ll: String, mm: String, nn: String, oo: String, pp: String, qq: String, rr: String, ss: String, tt: String, uu: String, vv: String)
  case class surveyResponse(QuesNbr: String,Ques_Desc: String,Problem_Category: String, Category_Name: String, Problem_Sub_Category: String, Sub_Category_Name: String, Code1: String, Comment: String, Condition: String, Responsibility: String, Problem_Severity: String, New_To_Toyota_Family: String, Comment_Control: String)
  case class customer(Height: String, Weight: String, Riders: String, Gender: String, Age: String)
  case class vehicle(Vin: String, Model_number: String, Model_Year: String, Model_Name: String, Report_month: String, Plant: String, Engine: String, Line_off_date: String, Code2: String, Code3: String, Code4: String, Code5: String, Code6: String, Code7: String, Body_style: String, Count: String, Wave: String, Sale_Month: String, Region: String, District: String, Dealer: String, RDRDate: String, ProdYRMM: String, Tires: String, Audio_Unit: String, Mileage: String, Survey_Version: String, Katashiki: String, Report_Week: String, Exterior_Color: String, Interior_Color: String)
  case class audit(User: String, Create_ts: String, Update_ts: String)
  case class Survey(_id: String, SurveyID_Comm: String, SurveyType: String, Customer: customer, Vehicle: vehicle, SurveyResponseComments: Array[surveyResponse], Audit: audit)
  case class surveyResponse_feature(QuesNbr: String,Ques_Desc: String,Problem_Category: String, Category_Name: String, Problem_Sub_Category: String, Sub_Category_Name: String, Code1: String, Comment: String, Condition: String, New_To_Toyota_Family: String)
  case class customer_feature(Height: String, Weight: String, Riders: String, Gender: String, Age: String)
  case class vehicle_feature(Vin: String, Model_number: String, Model_Year: String, Model_Name: String, Report_month: String, Plant: String, Engine: String, Line_off_date: String, Code2: String, Code3: String, Code4: String, Code5: String, Code6: String, Code7: String, Body_style: String, Count: String, Wave: String, Sale_Month: String, Region: String, District: String, Dealer: String, RDRDate: String, ProdYRMM: String, Tires: String, Audio_Unit: String, Mileage: String, Survey_Version: String, Katashiki: String, Report_Week: String)
  case class audit_feature(User: String, Create_ts: String, Update_ts: String)
  case class Survey_feature(_id: String, SurveyID_Feat: String, SurveyType: String, Customer: customer_feature, Vehicle: vehicle_feature, SurveyResponse_feature: Array[surveyResponse_feature], Audit: audit_feature)
  case class surveyResponse_resp(QuesNbr: String,Ques_Desc: String,Problem_Category: String, Problem_Sub_Category: String, Question_Response: String, Comments: String, Vehicle_Satisfaction: String, Value_Satisfaction: String, Dealer_Satisfaction: String, Quality_Satisfaction: String, Likely_to_Service: String, Likely_to_Recommend: String, Likely_to_Purchase: String, New_To_Toyota_Family: String)
  case class customer_resp(Height: String, Weight: String, Riders: String, Gender: String, Age: String, Cell_Phone: String, Cell_Provider: String,Customer_State: String, Customer_Zip: String )
  case class vehicle_resp(Vin: String, Model_number: String, Model_Year: String, Model_Name: String,Report_Month: String, Plant: String, Engine: String, Line_off_date: String, Body_style: String, Wave: String, Sale_Month: String, Region: String, Tires: String, Audio: String, Mileage: String,Report_Week: String, Survey_Version: String, Katashiki: String)
  case class audit_resp(User: String, Create_ts: String, Update_ts: String)
  case class Survey_resp(_id_resp: String, SurveyID_Resp: String, SurveyType: String, Customer_Resp: customer_resp, Vehicle_Resp: vehicle_resp, SurveyResponse: Array[surveyResponse_resp], Audit: audit_resp)
  //case class Final_Collection(SurveyID: String, SurveyType: String, Customer: List[String], Vehicle: List[String], PQS_Comments: List[String], Survey_Response: List[String], PQS_Comments_Featured: List[String] , User: String, Create_ts: String, Update_ts: String)

  def main(args: Array[String]) {

   
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
      
       val questionMap: mutable.Map[String, String] = new mutable.HashMap[String, String]
    val bd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQSCodeSpecs.txt").map(x => x.split("\\|")).collect.map(x => {
      val key = x(3).toString().trim() + x(4).toString().trim()
      val value = x(0).toString().trim()
      questionMap += key -> value
    })
  
     val bdQuestion= sparkSession.sparkContext.broadcast(bd(0))
     
   val questionMap1: mutable.Map[String, String] = new mutable.HashMap[String, String]   
  val cd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/Question_Desc.csv").map(x=>x.split(",")).collect.map(x=> {
    val key = x(0).toString().trim()
    val value = x(1).toString().trim()
    questionMap1 += key -> value  
  })


  
  val cdQuestion = sparkSession.sparkContext.broadcast(cd(0))
      
      
       val rdd_resp = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_Response.txt")
       
       val respfirst = rdd_resp.first();
   
       val resprdd = rdd_resp.filter(row => row != respfirst)   
       
    val xx_resp = resprdd.map(x => x.split("\\|"))
    

    val zz_resp = xx_resp.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40)))

    import sparkSession.implicits._

    val rddPaired_resp = zz_resp.map(x => (x(36).toString(), x))

    val rddSurvey_resp = rddPaired_resp.groupByKey().map(x => {

      println("X_1......." + x._1)

      val SurveyID_Resp = x._1.trim()

      val list1 = x._2.toArray

      val SurveyType = "PQS Survey"
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
       
       
      val responseArr_resp = x._2.toArray.map(x => {
        
        val Ques_NO_Resp = x(37).toString().trim().replaceAll("^0+", "")
        
       val Ques_Desc_Resp = cdQuestion.value.get(Ques_NO_Resp).getOrElse("No Description Found")
        
        surveyResponse_resp(x(37).toString().trim(),Ques_Desc_Resp,x(4).toString().trim(), x(5).toString().trim(), x(6).toString().trim(), x(23).toString().trim(), x(25).toString().trim(), x(26).toString().trim(), x(27).toString().trim(), x(28).toString().trim(), x(29).toString().trim(), x(30).toString().trim(), x(31).toString().trim(), 
              x(32).toString().trim() )

      })

      val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts

      Survey_resp(_id, SurveyID_Resp, SurveyType, customer_resp(Height, Weight, Rides, Gender, Age, Cell_Phone, Cell_Provider,Customer_State,Customer_Zip), vehicle_resp(Vin, Model_Number, Model_Year, Model_Name, Report_Month, Plant, Engine, Line_Off_Date, Body_Style, Wave, Sale_Month, Region, Tires, Audio, Mileage, Report_Week,Survey_Version, Katashiki), responseArr_resp.toArray, audit_resp(User, Create_ts, Update_ts))

    })

    val dfSurvey_resp = rddSurvey_resp.toDF()
    

    
    val datanew = dfSurvey_resp.as[Survey_resp]
       
    println("this is the dataset");
     
       datanew.show();

    dfSurvey_resp.printSchema()
    dfSurvey_resp.show
      
      
      

    val rdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_Comments.txt")
    
        val commfirst = rdd.first();
   
       val commrdd = rdd.filter(row => row != commfirst) 
    
    val xx = commrdd.map(x => x.split("\\|"))

    val zz = xx.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(35), x(36), x(37), x(38), x(39), x(40), x(41), x(42), x(43), x(44), x(45), x(46), x(47), x(48)))

    import sparkSession.implicits._

    val rddPaired = zz.map(x => (x(46).toString(), x))

    val rddSurvey = rddPaired.groupByKey().map(x => {

      println("X_1......." + x._1)

      val SurveyID_Comm = x._1.trim()

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
        
         val Ques_Number = bdQuestion.value.get(x(5).toString().trim()+x(7).toString().trim()).getOrElse("No question found")
    
    val Ques_NO_Comm = Ques_Number.replaceAll("^0+", "")
    
      
         val Ques_Desc_Comm = cdQuestion.value.get(Ques_NO_Comm).getOrElse("No Description Found")
     

        surveyResponse(Ques_Number,Ques_Desc_Comm,x(4).toString().trim(), x(5).toString().trim(), x(6).toString().trim(), x(7).toString().trim(), x(12).toString().trim(), x(28).toString().trim(), x(29).toString().trim(), x(41).toString().trim(), x(42).toString().trim(), x(43).toString().trim(), x(44).toString().trim())

      })

      val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts

      Survey(_id, SurveyID_Comm, SurveyType, customer(Height, Weight, Rides, Gender, Age), vehicle(Vin, Model_Number, Model_Year, Model_Name, Report_Month, Plant, Engine, Line_Off_Date, Code2, Code3, Code4, Code5, Code6, Code7, Body_Style, Count, Wave, Sale_Month, Region, District, Dealer, RDRDate, ProdYRMM, Tires, Audio_Unit, Mileage, Survey_Version, Katashiki, Report_Week, Exterior_Color, Interior_Color), responseArr.toArray, audit(User, Create_ts, Update_ts))

    })

    val dfSurvey = rddSurvey.toDF()
    
 
  

    dfSurvey.printSchema()
    dfSurvey.show
   // dfSurvey.write.format("com.mongodb.spark.sql").mode("append").save()

  
  

    val rdd_feature = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_FeatureComments.txt")
    
       val featurefirst = rdd_feature.first();
   
       val featurerdd = rdd_feature.filter(row => row != featurefirst) 
    
    val xx_feature = featurerdd.map(x => x.split("\\|"))

    val zz_feature = xx_feature.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40), x(41), x(42), x(43)))

    val rddPaired_feature = zz_feature.map(x => (x(42).toString(), x))
    

    val rddSurvey_feature = rddPaired_feature.groupByKey().map(x => {

      println("X_1......." + x._1)
      
      
      val SurveyID_Feat = x._1.trim()

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
      val responseArr_feature = x._2.toArray.map(x => {
        
      val Ques_NO_Feature = x(43).toString().trim().replaceAll("^0+", "")
        
       val Ques_Desc_Feature = cdQuestion.value.get(Ques_NO_Feature).getOrElse("No Description Found")

        surveyResponse_feature(x(43).toString().trim(),Ques_Desc_Feature,x(4).toString().trim(), x(5).toString().trim(), x(6).toString().trim(), x(7).toString().trim(), x(12).toString().trim(), x(28).toString().trim(), x(29).toString().trim(), x(41).toString().trim())

      })

      val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts

      Survey_feature(_id, SurveyID_Feat, SurveyType, customer_feature(Height, Weight, Rides, Gender, Age), vehicle_feature(Vin, Model_Number, Model_Year, Model_Name, Report_Month, Plant, Engine, Line_Off_Date, Code2, Code3, Code4, Code5, Code6, Code7, Body_Style, Count, Wave, Sale_Month, Region, District, Dealer, RDRDate, ProdYRMM, Tires, Audio_Unit, Mileage, Survey_Version, Katashiki, Report_Week), responseArr_feature.toArray, audit_feature(User, Create_ts, Update_ts))

    })

    val dfSurvey_feature = rddSurvey_feature.toDF()

    dfSurvey_feature.printSchema()
    dfSurvey_feature.show
  //  dfSurvey_feature.write.format("com.mongodb.spark.sql").mode("append").save()

  
   
   // dfSurvey_resp.write.format("com.mongodb.spark.sql").mode("append").save()

     val joinsdf = dfSurvey.join(dfSurvey_feature,dfSurvey("SurveyID_Comm") === dfSurvey_feature("SurveyID_Feat")).join(dfSurvey_resp,dfSurvey("SurveyID_Comm") === dfSurvey_resp("SurveyID_Resp"))

      joinsdf.show();
    /*  
      val SurveyType = "PQS_SURVEY"
      
      val SurveyID = joinsdf.select("SurveyID")
      
      val Customer = joinsdf.select("customer_resp").collectAsList()
      
      val Vehicle = joinsdf.select("vehicle_resp").collectAsList()
      
      val PQS_Comments = joinsdf.select("SurveyResponseComments").collectAsList()
      
      val Survey_Response = joinsdf.select("SurveyResponse").collectAsList()
      
      val PQS_Comments_Featured = joinsdf.select("SurveyResponse_feature").collectAsList() 
        val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts
      
 // Final_Collection( SurveyID, SurveyType, Customer, Vehicle, PQS_Comments, Survey_Response, PQS_Comments_Featured , User,Create_ts,Update_ts);   
*/
       val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts
      
   val  Final_Collection1 = joinsdf.select("_id_resp","SurveyID_Resp","Customer_Resp","Vehicle_Resp","SurveyResponseComments","SurveyResponse","SurveyResponse_feature")
 

   
   val Final_Coll = Final_Collection1.withColumn("User",lit(User))
                         .withColumn("Create_ts",lit(Create_ts))
                         .withColumn("Update_ts",lit(Update_ts))
                         .withColumn("SurveyType",lit("PQS_Survey"))
                    
   
   val Final_Survey_Collection = Final_Coll.select("_id_resp","SurveyID_Resp","SurveyType","Customer_Resp","Vehicle_Resp","SurveyResponseComments","SurveyResponse","SurveyResponse_feature","User","Create_ts","Update_ts")
  
   
   val Final_Survey_Collection_Renamed = Final_Survey_Collection.toDF("_id","SurveyID","SurveyType","Customer","Vehicle","SurveyResponseComments","SurveyResponse","SurveyResponse_feature","User","Create_ts","Update_ts")
  
   Final_Survey_Collection_Renamed.show()
   
  //  Final_Survey_Collection_Renamed.write.format("com.mongodb.spark.sql").mode("append").option("replaceDocument", "false").save()
 
  
  }

}
