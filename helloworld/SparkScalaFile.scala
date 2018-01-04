package helloworld

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
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SparkScalaFile {
  
  case class MyDataframe(Vin:String,ModelNumber:String,ModelYear:String,ModelName:String,Verbatim:String )
  
  def main(args: Array[String]){
    
     val sparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkScalaFile")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.testCollection1")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.FinalCollectionSurveyV7")
   /*   .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsin")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsout1")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqsresponse")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqsresponseout")*/
      .getOrCreate()
      import sparkSession.implicits._

     
  val Structure = 
    StructType(
        StructField("RetailSalesMonth",StringType, true) ::
        StructField("RetailSalesYear",StringType,true) ::
        StructField("DealerNumer", StringType, true) ::
        StructField("LexusToyotaIndicator", StringType, true) ::
        StructField("RegionCode", StringType, true) ::
        StructField("DistrictCode", StringType, true) ::
        StructField("TerminationCode", StringType, true) ::
        StructField("FinancialSalesYear", StringType, true) ::
        StructField("FinancialSalesMonth", StringType, true) ::
        StructField("AccountCode", StringType, true) ::
        StructField("AccountTypeCode", StringType, true) ::
        StructField("AccountDescription", StringType, true) ::
        StructField("AccountUnitMeasurementCode", StringType, true) ::
        StructField("AccountAggregatedProductionCode", StringType, true) ::
        StructField("AccountAggregatedRuleIndicator", StringType, true) ::
        StructField("AccountAmountsMTD", StringType, true) ::
        StructField("AccountAmountsYTD", StringType, true) ::
        Nil)
     
    val DealerData = sparkSession.sparkContext.textFile("file:///C:/Streaming_Data/dealer_financial_data_2017_11_1_11_1_T.txt");
     
     val dealerDatafirst = DealerData.first();
     
     val FinalDealerData = DealerData.filter(row => row != dealerDatafirst )
     
     val FinalDealer = FinalDealerData.map{x => x.split("\\|")}
     
     val CollectedList = FinalDealer.collect()
     
   //  CollectedList.foreach(println)
     
     println("take function coming up....")
     
 //    CollectedList.take(10).foreach(println)
     
     val FinalDeal = FinalDealer.map{x=>
     
     Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16))  
     }
     
     val DealerDF = sparkSession.createDataFrame(FinalDeal, Structure)
     
     def addOneToElements(elements: Seq[Int]) = elements.map(element => element + 1)
     
  //   def addElements(elements: String) = elements.map(element => element.concat("Added"))
     
     sparkSession.udf.register("plusOneInt", addOneToElements(_:Seq[Int]):Seq[Int])
     
   //  sparkSession.udf.register("addString",addElements(_: String):String)
     
    DealerDF.createOrReplaceTempView("MynewTable");
     
     val myseq = Seq(1,2,3)
     
  val mydf2 =   sparkSession.sql("select *,addString(RegionCode) as AddedRegionCode from MynewTable");
  
  mydf2.show(5);
     
     
     
  //DealerDF.show(5)
  
  //DealerDF.select("AccountAmountsYTD").show()
  


  
  val mydf = DealerDF.schema
  
  //println(mydf);
     
    // DealerDF.rdd.saveAsTextFile("file:///C:/Streaming_Data/output_dealer.txt")
     
    // DealerDF.write.csv("file:///C:/Streaming_Data/output_dealer.csv")
     
  //   format("csv").save("file:///C:/Streaming_Data/output_dealer.csv");
    
     
     //val NewDealerDF = FinalDealerData.toDF(Structure)
        
        
        
        //srdd_resp.collect.foreach(println);*/
    
  }
  
}