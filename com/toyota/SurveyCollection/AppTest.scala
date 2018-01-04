package com.toyota.SurveyCollection
import org.apache.spark.sql.SparkSession
import com.toyota.SurveyCollection.pqssurvey
import org.apache.spark.sql.Encoders



object AppTest{
  
  
  val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/db1.testCollection1")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/db1.testCollection2")
      .getOrCreate()
      
      def main(args: Array[String])
  {
    
      val pqs2Com1:Comments=new pqssurvey with Serializable
      val x = pqs2Com1.process(sparkSession)
      x.show()
     
       val temp:Comments=new pqssurvey with Serializable
      val y = temp.process1(sparkSession)
      y.show()
       
       val pqs2resp1:Comments=new pqssurvey with Serializable
      val z = pqs2resp1.process3(sparkSession)
      z.show()
      
      val Df1 = x.join(y,x("SurveyID") === y("SurveyID"))

      Df1.show();
      
      val Df2 = Df1.join(z,Df1("SurveyID") === z("SurveyID"))
      
      Df2.show();
      // Df1.write.format("com.mongodb.spark.sql").mode("append").save()
       // Df2.write.format("com.mongodb.spark.sql").mode("append").save()
      

  }     
      
}