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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.MongoClient
import scala.collection.immutable.IndexedSeq
import org.mongodb.scala._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._ 
    

object myload {
  
  def main(args: Array[String]){
    
    println("Hello world!");
    
          val sparkSession = SparkSession.builder()
      .master("local")
     
      
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.testCollection1")
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.JoinedCollection")
   /*   .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsin")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqs2featurecommentsout1")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.pqsresponse")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.pqsresponseout")*/
      .getOrCreate()
      
    
  //sparkSession.sparkContext.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://example.com/database.collection"))) 
 // Uses the ReadConfig method to get the collection information which can be loaded to a dataframe using MongoSpark
      
       val readConfig1 = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/Survey.Survey_Response"))
       
       val readConfig2 = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/Survey.Survey_Comments"))
        
       val readConfig3 = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/Survey.Survey_FeatComments"))
       
       val respinfo=MongoSpark.load(sparkSession,readConfig1);
       respinfo.createOrReplaceTempView("Response");
       
       val comminfo = MongoSpark.load(sparkSession,readConfig2);
       comminfo.createOrReplaceTempView("Comments");
      
       val featinfo = MongoSpark.load(sparkSession,readConfig3);
       featinfo.createOrReplaceTempView("Feature")
       
       /// Joining three tables to form a single collection 
     val collectionfinal =  sparkSession.sql("select * from Response a,Comments b,Feature c where a.VIN=b.VIN and b.VIN=c.VIN");
     collectionfinal.write.format("com.mongodb.spark.sql").mode("append").option("replaceDocument", "false").save()
  }
  
}