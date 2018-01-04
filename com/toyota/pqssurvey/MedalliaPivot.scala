/*package com.toyota.pqssurvey

import java.io._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.storage._
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.storage._
import org.apache.spark.sql.execution._
import scala.collection.mutable.ListBuffer
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import java.util.Calendar
import org.apache.spark.sql.functions.{ lit }
import org.apache.spark.api.java.StorageLevels
import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.functions.udf
import scala.collection.immutable.ListMap

/**
 * <pre>
 * <b>Name :</b> MedalliaCustomerSurveyResponse.scala
 * <b>Description :</b> This class is responsible to
 * 	- Read the Medallia input files
 *  - Separate the Medallia pivot data by applying transformations.
 *  - Save the data in parquet format.
 *
 * <b>References :</b> None
 * @author : Cognizant
 *
 * <pre>
 * Development History: Version Changes Changed By
 * -----------------------------------------------------
 * 1.0 Initial version
 * </pre>
 */

object MedalliaPivot {
  
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
 //   implicit val config = getUserConfig(args, LEXUS_MED_PIVOT)

    val appName = "LEXUS SURVEY"
    implicit val sparkSession = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()
    try {
      generateSurveyResponseFlatFile
    } catch {
      case e: Exception =>
        sparkSession.stop()
        
    } finally {
      sparkSession.stop()
    }
  }

  /**
   * This method is used to get the input data from Medallia, apply transform, pivot and save as parquet file.
   */
  def generateSurveyResponseFlatFile()(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._
 //   val sysDateTime = DateFunctions.timeNow(DateFormats.YYYYMMDDHHMISS)

    val customerSrvyResponseDF = sparkSession.sparkContext.textFile("file:///C:/Users/402960/PQS_Response.txt")
    customerSrvyResponseDF.repartition(300).createOrReplaceTempView(LEXUS_MCSR_INPUT)

    val listDispDF = sparkSession.read.parquet(config.getString(LEXUS_MED_LIST_DISP))
    listDispDF.repartition(300).createOrReplaceTempView(LEXUS_MLD_INPUT)

    val medalliaStgSrvyQuestionDF = sparkSession.read.parquet(config.getString(LEXUS_MED_STG_SRVY_QUEST))
    medalliaStgSrvyQuestionDF.createOrReplaceTempView(LEXUS_MED_STG_SRV_QUEST_TEMP_VIEW)

    val maritzSrvyRspns = sparkSession.read.parquet(config.getString(LEXUS_MARITZ_SURVEY_RESPONSE_INSTANCE))
      .repartition(300).createOrReplaceTempView(LEXUS_MTZ_SRVY_RSPNS_TEMP_VIEW)

    val maritzDealerStaff = sparkSession.read.parquet(config.getString(LEXUS_MARITZ_DEALER_STAFF))
      .createOrReplaceTempView(LEXUS_MTZ_DLR_STAFF_TEMP_VIEW)

    val getSrvySeqId = udf(getSurveySeqId _)

    val updateSurveyType = udf(updateSrvyType _)

    val customerRespData = sparkSession.sql(joinQueryWithRespListDisp)

    customerRespData.persist(StorageLevels.MEMORY_AND_DISK_SER)
    
    val customerRespDataWithSeqId = customerRespData.withColumn(LEXUS_SRVY_TYPE, updateSurveyType('survey_type_txt))
      .withColumn(LEXUS_RECORD_CREATE_DT, lit(sysDateTime)).withColumn(LEXUS_RECORD_CREATE_SOURCE, lit(sysDateTime))
      .withColumn(LEXUS_RECORD_LAST_UPDATE_DT, lit(sysDateTime)).withColumn(LEXUS_RECORD_LAST_UPDATE_SOURCE, lit(sysDateTime))
    customerRespDataWithSeqId.createOrReplaceTempView(LEXUS_CUST_RESP_DATA_TEMP_VIEW)

    val compData = sparkSession.sql(joinQueryWithQuest)
    //compData.collect.foreach(println)
    val groupedBySrvyPopIdDF = compData.filter(compData(LEXUS_SRVY_TYPE) !== lit(LEXUS_EMPTY)).repartition(700).rdd.groupBy(f => (getS(0)(f))) //survey population id
    groupedBySrvyPopIdDF.persist(StorageLevels.MEMORY_AND_DISK_SER)
    
    val surveyByIdRDDMapC = groupedBySrvyPopIdDF.map { f =>
      val popId = f._1
      val rowList = f._2.toList
      val headerRow = rowList.head
      val fulfilmentVendor = getS(16)(headerRow) //Medallia
      val surveyTypePrefix = getS(9)(headerRow) //CPO
      val questionGroupNM = rowList.groupBy(row => getS(10)(row)) //groupingnm
      val colVal = surveyTypePrefix match {
        case (LEXUS_CPO | LEXUS_SLS) => processSLSCPOAnswers(questionGroupNM, fulfilmentVendor, getS(4)(headerRow))
        case LEXUS_SVC => processSVCAnswers(questionGroupNM, fulfilmentVendor, getS(4)(headerRow))
      }
      val allAnswers = addEliteIndex(7, colVal)
      val answerListAll = addHeaderRow(headerRow) ++ ListMap(allAnswers.toSeq.sortBy(_._1): _*).filter(_._1 > 0).values.toSeq ++ addPQSSColList(headerRow)
      Row(answerListAll: _*)
    }
    val Survey_Response_File = config.getString(LEXUS_MCSR_OUTPUT)
    val surveyRDDFinalOut = sparkSession.createDataFrame(surveyByIdRDDMapC, surveyMedalliaSchStruct)
    FileHandler.saveSRDDasFiles(surveyRDDFinalOut, Survey_Response_File, config)
  }

  
  
}*/