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
import com.mongodb.spark.config._
import org.apache.spark.streaming._



object hello {
  
  case class Match(matchId: Int, player1: String, player2: String)
case class Player(name: String, birthYear: Int)
  
    def main(args: Array[String]) {
  
  println("Hello");
  
   val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Survey.PqsComments")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Survey.PqsCommentsOut")
      .getOrCreate()
  
  val matches = Seq(
  Match(1, "John Wayne", "John Doe"),
  Match(2, "Ive Fish", "San Simon")
)

val players = Seq(
  Player("John Wayne", 1986),
  Player("Ive Fish", 1990),
  Player("San Simon", 1974),
  Player("John Doe", 1995)
)

val Df1 = sparkSession.createDataFrame(matches)

val Df2 = sparkSession.createDataFrame(players)

Df1.show()

Df2.show()




val Df3 = Df1.join(Df2,Df1("player1") === Df2("name"))

Df3.show();

    }
}