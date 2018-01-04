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

object PQSConnectServices {
  
   case class all(a: String, b: String, c: String, d: String, e: String, f: String, g: String, h: String, i: String, j: String, k: String, l: String, m: String, n: String, o: String, p: String, q: String, r: String, s: String, t: String, u: String, v: String, w: String, x: String, y: String, z: String, aa: String, bb: String, cc: String, dd: String, ee: String, ff: String, gg: String, hh: String, ii: String, jj: String, kk: String, ll: String, mm: String, nn: String, oo: String, pp: String, qq: String, rr: String)

  case class Survey_Comments(BingComment: String, iHeartRadioComment: String, MovieTicketsComment: String, OpenTableComment: String, PandoraComment: String,FuelPricesComment: String,
      SportsComment: String, StockComment: String, TrafficComment: String, WeatherComment: String,YelpComment: String, FacebookPlacesComment: String, FeatureComment: String )
  case class Survey_Details(Survey_unique_ID: String, Selling_dealership_code: String, EntuneSmartphoneProblemBox: String, EntuneSmartphoneProblemComment: String, 
      Bing: String, iHeartRadio: String, MovieTickets: String, OpenTable: String, Pandora: String, FuelPrices: String,  Sports: String, Stocks: String, Traffic: String,
      Weather: String, Yelp: String, FacebookPlaces: String)
  case class customer(City: String, State: String, Gender: String, BirthYear: String, Purchase_date: String,Submit_date: String,Wave_sent: String)
  case class vehicle(Model_Name: String, Model_Year: String) 
  case class audit(User: String, Create_ts: String, Update_ts: String)
  case class Survey(_id: String, SurveyID: String, SurveyType: String, Customer: customer, Vehicle: vehicle,Survey_Response:Survey_Details, Survey_Comments: Survey_Comments, Audit: audit)




  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.PqsFeatureComments")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.PQSConnectServices3")
      .getOrCreate()

  val rdd = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQSConnectServices.csv")
    val xx = rdd.map(x => x.split(","))

    val zz = xx.map(x =>

      Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21), x(22), x(23), x(24), x(25), x(26), x(27), x(28), x(29), x(30), x(31), x(32), x(33), x(34), x(35), x(36), x(37), x(38), x(39), x(40)))
     

    import sparkSession.implicits._

    val rddPaired = zz.map(x => (x(40).toString().trim()+x(0).toString().trim(), x))
    

    val rddSurvey = rddPaired.groupByKey().map(x => {

      println("X_1......." + x._1)
      
      
      val SurveyID = x._1.trim()

      val list1 = x._2.toArray

      val SurveyType = "PQSConnectServices"
      val _id = x._1.trim() + "_" + SurveyType
      
      val Survey_unique_ID= list1(0)(1).toString().trim()
val Selling_dealership_code= list1(0)(2).toString().trim()
val City= list1(0)(3).toString().trim()
val State= list1(0)(4).toString().trim()
val Gender= list1(0)(5).toString().trim()
val BirthYear= list1(0)(6).toString().trim()
val Purchase_date= list1(0)(7).toString().trim()
val Submit_date= list1(0)(8).toString().trim()
val Wave_sent= list1(0)(9).toString().trim()
val SurveyVersion= list1(0)(10).toString().trim()
val Model_Name= list1(0)(11).toString().trim()
val Model_Year= list1(0)(12).toString().trim()
val EntuneSmartphoneProblemBox= list1(0)(13).toString().trim()
val EntuneSmartphoneProblemComment= list1(0)(14).toString().trim()
val Bing= list1(0)(15).toString().trim()
val iHeartRadio= list1(0)(16).toString().trim()
val MovieTickets= list1(0)(17).toString().trim()
val OpenTable= list1(0)(18).toString().trim()
val Pandora= list1(0)(19).toString().trim()
val FuelPrices= list1(0)(20).toString().trim()
val Sports= list1(0)(21).toString().trim()
val Stocks= list1(0)(22).toString().trim()
val Traffic= list1(0)(23).toString().trim()
val Weather= list1(0)(24).toString().trim()
val Yelp= list1(0)(25).toString().trim()
val FacebookPlaces= list1(0)(26).toString().trim()
val BingComment= list1(0)(27).toString().trim()
val iHeartRadioComment= list1(0)(28).toString().trim()
val MovieTicketsComment= list1(0)(29).toString().trim()
val OpenTableComment= list1(0)(30).toString().trim()
val PandoraComment= list1(0)(31).toString().trim()
val FuelPricesComment= list1(0)(32).toString().trim()
val SportsComment= list1(0)(33).toString().trim()
val StockComment= list1(0)(34).toString().trim()
val TrafficComment= list1(0)(35).toString().trim()
val WeatherComment= list1(0)(36).toString().trim()
val YelpComment= list1(0)(37).toString().trim()
val FacebookPlacesComment= list1(0)(38).toString().trim()
val FeatureComment= list1(0)(39).toString().trim()
val ncsid= list1(0)(40).toString().trim()

  val User = "Spark"
      val format = new SimpleDateFormat("yyyyMMddHHmmss")
      val Create_ts = format.format(Calendar.getInstance().getTime())
      val Update_ts = Create_ts


      Survey(_id, SurveyID, SurveyType, customer(City, State, Gender, BirthYear, Purchase_date,Submit_date,Wave_sent), vehicle(Model_Name, Model_Year),
          Survey_Details(Survey_unique_ID, Selling_dealership_code, EntuneSmartphoneProblemBox, EntuneSmartphoneProblemComment, Bing, iHeartRadio, MovieTickets, OpenTable, Pandora, FuelPrices,  Sports, Stocks, Traffic,Weather, Yelp, FacebookPlaces),
          Survey_Comments(BingComment, iHeartRadioComment, MovieTicketsComment, OpenTableComment, PandoraComment,FuelPricesComment,SportsComment, StockComment, TrafficComment, WeatherComment,YelpComment, FacebookPlacesComment, FeatureComment ),
          audit(User, Create_ts, Update_ts))
    })

val dfSurvey = rddSurvey.toDF()

    dfSurvey.printSchema()
    dfSurvey.show
    dfSurvey.write.format("com.mongodb.spark.sql").mode("append").save()

  }
  
}