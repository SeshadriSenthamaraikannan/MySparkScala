package com.toyota.spark_algebird

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.Status
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.twitter._
import twitter4j.Status
import org.apache.spark.streaming.dstream.ReceiverInputDStream




object PopularTweets {

  def main(args: Array[String]) {
    val (ssc: StreamingContext, tweets: ReceiverInputDStream[Status]) =
      initStreamContext("PopularTweets", 1)
    popularTweets(tweets)
    startApp(ssc)
  }

  /**
    * Print top 10 tweets in the stream
    * @param tweets Stream of tweets
    */
  private def popularTweets(tweets: ReceiverInputDStream[Status]) = {
    val statuses = tweets.map(status => status.getText)
    val tweetWords = statuses.flatMap(tweetText => tweetText.split(" "))
    val hashtags = tweetWords.filter(word => word.startsWith("#"))

    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    //5 minute window sliding every one second
    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy({ case (_, count) => count }, ascending = false))
    sortedResults.print
  }
  
/*    private def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }*/

  private def setupTwitter(): Unit = {
    import scala.io.Source

    for (line <- Source.fromFile("twitter.conf").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /**
    * Path to checkpoint directory
    */
  private val CHECKPOINT_PATH = "checkpoint"
  
  def startApp(ssc: StreamingContext): Unit = {
    ssc.checkpoint(CHECKPOINT_PATH)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Get StreamingContext and ReceiverInputDStream for streaming tweets
    * @param appName application name
    * @return the tuple of StreamingContext ReceiverInputDStream
    */
  def initStreamContext(appName: String, seconds: Int): (StreamingContext, ReceiverInputDStream[Status]) = {
    setupTwitter()
    val ssc = new StreamingContext("local[*]", appName, Seconds(seconds))
   // setupLogging()
    val tweets = TwitterUtils.createStream(ssc, None)
    (ssc, tweets)
  }
}

  


 