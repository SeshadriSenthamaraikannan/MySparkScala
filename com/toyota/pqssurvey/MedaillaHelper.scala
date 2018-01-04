/*package com.toyota.pqssurvey

import org.apache.spark.sql.Row
import com.toyota.customer360.ecsc.util.RowGet._
import com.toyota.customer360.ecsc.util.StringUtil._
import com.toyota.customer360.ecsc.util.AppConfig
import com.toyota.customer360.ecsc.util.LexusConstants._
import org.apache.spark.sql.types._
import com.toyota.customer360.ecsc.util.DateFunctions._

object MedaillaHelper {
  
  val no_response = LEXUS_NOT_RESPONDED
  val questconfig = AppConfig.loadConfig(LEXUS_QUEST_CONF)
  val answerconfig = AppConfig.loadConfig(LEXUS_ANS_CONF)
  val multiFieldDelimiter = AppConfig.loadConfig(LEXUS_APP_CONF).getConfig(LEXUS_COMMON).getString(LEXUS_MULTI_FIELD_DELIM)

  /**
   * Schema for the output column structure for Medallia Pivot
   */
  val surveyMedalliaSchStruct =
    StructType(
      StructField("survey_instance_system_id", StringType, true) ::
        StructField("survey_population_id", StringType, true) ::
        StructField("cust_ccd_id", StringType, true) ::
        StructField("srvy_received_dt", StringType, true) ::
        StructField("survey_type_prefix_cd", StringType, true) ::
        StructField("division_name", StringType, true) ::
        StructField("prod_transact_dt", StringType, true) ::
        StructField("srvy_sent_dt", StringType, true) ::
        StructField("fulfilment_vendor_nm", StringType, true) ::
        StructField("prod_vin_txt", StringType, true) ::
        StructField("dea_dealer_system_id", StringType, true) ::
        StructField("osat_score", StringType, true) :: //survey common fields
        StructField("elite_return_score", StringType, true) ::
        StructField("elite_recommend_score", StringType, true) ::
        StructField("med_osat_response", StringType, true) :: //medallia survey common fields
        StructField("med_elite_return_response", StringType, true) ::
        StructField("med_elite_recommend_response", StringType, true) ::
        StructField("med_elite_index", StringType, true) ::
        StructField("med_elite_comment", StringType, true) ::
        StructField("veh_replacement_yn", StringType, true) :: //medallia survey sales fields
        StructField("veh_replacement_mkmdl", StringType, true) ::
        StructField("attraction", StringType, true) ::
        StructField("attraction_comment", StringType, true) ::
        StructField("website_experience_score", StringType, true) ::
        StructField("website_experience_options", StringType, true) ::
        StructField("website_experience_comment", StringType, true) ::
        StructField("consult_experience_score", StringType, true) ::
        StructField("consult_experience_options", StringType, true) ::
        StructField("consult_experience_comment", StringType, true) ::
        StructField("pay_experience_score", StringType, true) ::
        StructField("pay_experience_options", StringType, true) ::
        StructField("pay_experience_comment", StringType, true) ::
        StructField("fin_mgr_experience_score", StringType, true) ::
        StructField("fin_mgr_experience_options", StringType, true) ::
        StructField("fin_mgr_experience_comment", StringType, true) ::
        StructField("veh_delivery", StringType, true) ::
        StructField("veh_delivery_exp_score", StringType, true) ::
        StructField("veh_delivery_exp_index", StringType, true) ::
        StructField("veh_delivery_exp_options", StringType, true) ::
        StructField("veh_delivery_exp_comment", StringType, true) ::
        StructField("dlr_experience_comment", StringType, true) ::
        StructField("veh_concerns_response", StringType, true) ::
        StructField("veh_concerns_options", StringType, true) ::
        StructField("veh_concerns_assist_lts", StringType, true) ::
        StructField("veh_concerns_assist_sc", StringType, true) ::
        StructField("veh_concerns_assist_sp", StringType, true) ::
        StructField("veh_concerns_assist_odp", StringType, true) ::
        StructField("service_work_type", StringType, true) :: //medallia survey service fields
        StructField("service_work_type_comment", StringType, true) ::
        StructField("service_experience_score", StringType, true) ::
        StructField("service_experience_options", StringType, true) ::
        StructField("service_experience_comment", StringType, true) ::
        StructField("dealership_satisfaction_score", StringType, true) ::
        StructField("dealership_satisfaction_comment", StringType, true) ::
        StructField("dealer_communication_sat_score", StringType, true) ::
        StructField("dealer_communication_sat_options", StringType, true) ::
        StructField("dealer_communication_sat_comment", StringType, true) ::
        StructField("vehicle_fix_first_time_response", StringType, true) ::
        StructField("vehicle_fix_first_time_index", StringType, true) ::
        StructField("vehicle_fix_first_time_options", StringType, true) ::
        StructField("vehicle_fix_first_time_comment", StringType, true) ::
        StructField("service_delivery_experience_score", StringType, true) ::
        StructField("service_delivery_experience_index", StringType, true) ::
        StructField("service_delivery_experience_options", StringType, true) ::
        StructField("service_delivery_experience_comment", StringType, true) ::
        StructField("after_service_satisfaction_response", StringType, true) ::
        StructField("after_service_satisfaction_score", StringType, true) ::
        StructField("dealership_experience", StringType, true) ::
        StructField("dealership_enrich", StringType, true) ::
        StructField("cust_salutation", StringType, true) :: //PQSS
        StructField("cust_first_nm", StringType, true) ::
        StructField("cust_middle_initial", StringType, true) ::
        StructField("cust_last_nm", StringType, true) ::
        StructField("cust_nm_suffix", StringType, true) ::
        StructField("dealer_nm", StringType, true) ::
        StructField("repair_order_no", StringType, true) ::
        StructField("repair_date", StringType, true) ::
        StructField("svc_consultant_spin", StringType, true) ::
        StructField("cust_response_date", StringType, true) ::
        StructField("svc_consultant_nm", StringType, true) ::
        StructField("ro_tech_spin", StringType, true) ::
        StructField("ro_tech_nm", StringType, true) ::
        StructField("tms_response_recd_date", StringType, true) ::
        StructField("record_create_dt", StringType, true) ::
        StructField("record_create_source", StringType, true) ::
        StructField("record_last_update_dt", StringType, true) ::
        StructField("record_last_update_source", StringType, true) ::
        Nil)

  /**
   * This method is used to map the survey population Id's to sales, service and cpo.
   */
  def getSurveySeqId(srvyType: String): String = {
    srvyType match {
      case LEXUS_SALES => LEXUS_SLS_ID
      case LEXUS_SERVICE => LEXUS_SVC_ID
      case LEXUS_CPO => LEXUS_CPO_ID
      case _ => LEXUS_EMPTY
    }
  }

  /**
   * This method is used to add the common columns for maritz and medallia
   */
  def addHeaderRow(headerRow: Row) =
    Seq(LEXUS_EMPTY,
      getS(0)(headerRow),
      getS(3)(headerRow),
      convertDateFormat(getS(25)(headerRow), DateFormats.YYYYMMDDHHMISSS, DateFormats.YYYYMMDD),
      getS(4)(headerRow),
      LEXUS_NAME,
      convertDateFormat(getS(22)(headerRow), DateFormats.YYYYMMDDHHMISSS, DateFormats.YYYYMMDD),
      convertDateFormat(getS(24)(headerRow), DateFormats.YYYYMMDDHHMISSS, DateFormats.YYYY_MM_DD),
      getS(16)(headerRow),
      getS(20)(headerRow),
      getS(23)(headerRow))

  /**
   * This method is used to add the PQSS column list
   */
  def addPQSSColList(headerRow: Row) =
    Seq(getS(26)(headerRow), getS(6)(headerRow), getS(27)(headerRow), getS(7)(headerRow),
      getS(28)(headerRow), getS(29)(headerRow), getS(30)(headerRow),
      convertDateFormat(getS(31)(headerRow), DateFormats.YYYYMMDDHHMISS, DateFormats.YYYYMMDD),
      getS(32)(headerRow),
      convertDateFormat(getS(33)(headerRow), DateFormats.YYYYMMDDHHMISS, DateFormats.YYYYMMDD),
      getS(34)(headerRow),
      getS(35)(headerRow),
      getS(36)(headerRow),
      convertDateFormat(getS(37)(headerRow), DateFormats.YYYYMMDDHHMISS, DateFormats.YYYYMMDD),
      convertDateFormat(getS(38)(headerRow), DateFormats.YYYYMMDDHHMISS, DateFormats.YYYYMMDD),
      convertDateFormat(getS(39)(headerRow), DateFormats.YYYYMMDDHHMISS, DateFormats.YYYYMMDD),
      convertDateFormat(getS(40)(headerRow), DateFormats.YYYYMMDDHHMISS, DateFormats.YYYYMMDD),
      convertDateFormat(getS(41)(headerRow), DateFormats.YYYYMMDDHHMISS, DateFormats.YYYYMMDD))

  /**
   * This method is used to update the survey type
   */
  def updateSrvyType(srvyType: String): String = {
    srvyType match {
      case (LEXUS_SALES | LEXUS_CPO) => LEXUS_SLS
      case LEXUS_SERVICE => LEXUS_SVC
      case _ => LEXUS_EMPTY
    }
  }

  /**
   * Select query used to select the required columns from list disposition and customer survey response table.
   * Maritz dealer staff and survey response tables are joined to obtain consultant and technician names.
   */
  val joinQueryWithRespListDisp = """select
    
   resp.survey_population_id,
   resp.survey_disp_cd,
   resp.hh_ccd_id,
   resp.cust_ccd_id,
   resp.survey_type_txt,
   resp.question_no,
   resp.response_type,
   resp.response_text,
   disp.customer_first_name,
   disp.customer_last_name,
   disp.model_name,
   disp.vin, 
   disp.customer_id,
   disp.survey_type,
   disp.vehicle_purchase_date,
   disp.dealer_no,
   disp.customer_sent_date, 
   resp.date_recvd_from_cust_dt,
   disp.salutation, 
   disp.customer_middle_name,
   disp.customer_name_suffix,
   disp.dealer_name,
   disp.repair_order_number,
   disp.repair_order_date,
   disp.consultant_spin_id,
   resp.date_recvd_from_cust_dt,
   disp.ro_technician_spin_id,
   disp.vendor_recieved_date,
   si.svc_consultant_nm,
   si.ro_tech_nm
    
   from
   medallia_list_disposition disp   
   left outer join
   medallia_customer_srvy_response resp 
   on 
   resp.survey_population_id=disp.survey_population_id
   left outer join
    (SELECT resp.survey_population_id, 
               resp.dealership_associate_spin  AS SVC_CONSULTANT_SPIN, 
               resp.fi_manager_technician_spin AS RO_TECH_SPIN, 
               da.consultant_nm                AS SVC_CONSULTANT_NM, 
               fi.consultant_nm                AS RO_TECH_NM ,max(da.row_audit_modified_dt),max(fi.row_audit_modified_dt)
        FROM   maritz_survey_rspns_instance_pq resp left outer join  maritz_dealer_staff_pq da
        on  Substr(resp.dealership_associate_spin, 4) = da.consultant_uid 
               left outer join maritz_dealer_staff_pq fi 
                       on Substr(resp.fi_manager_technician_spin, 4) = fi.consultant_uid
               group by resp.survey_population_id,resp.dealership_associate_spin,resp.fi_manager_technician_spin,da.consultant_nm,fi.consultant_nm) si
    on disp.survey_population_id = si.survey_population_id where disp.valid_indicator='Yes'"""
  //where resp.survey_population_id='370156192','106782961'
  /**
   * Select query to join the medallia question table and the previous join query based on fulfillment vendor.
   */
  val joinQueryWithQuest = """select 
    
    cust.survey_population_id,
    cust.survey_disp_cd,
    cust.hh_ccd_id,
    cust.cust_ccd_id,
    cust.survey_type_txt,
    cust.response_type,
    cust.customer_first_name,
    cust.customer_last_name,
    cust.model_name ,
    cust.survey_type,
    quest.grouping_nm, 
    quest.survey_question_no_txt,
    cust.question_no,
    cust.response_text,
    quest.qstn_question_txt,
    quest.survey_question_order_no,
    quest.fulfilment_vendor_nm,
    quest.survey_seq_id,
    quest.srvy_question_no_seq_txt,
    quest.measure_type_txt,
    cust.vin,  
    cust.customer_id,    
    cust.vehicle_purchase_date,
    cust.dealer_no,
    cust.customer_sent_date,
    cust.date_recvd_from_cust_dt,
    cust.salutation, 
    cust.customer_middle_name,
    cust.customer_name_suffix,
    cust.dealer_name,
    cust.repair_order_number,
    cust.repair_order_date,
    cust.consultant_spin_id,
    cust.date_recvd_from_cust_dt,
    cust.svc_consultant_nm,
    cust.ro_technician_spin_id,
    cust.ro_tech_nm,
    cust.vendor_recieved_date,
    cust.record_create_dt,
    cust.record_create_source,
    cust.record_last_update_dt,
    cust.record_last_update_source

    from custRespDataWithSeqId cust 
    left outer join medalliaStgSrvyQuest quest 
    on cust.question_no=quest.survey_question_no_txt
    and cust.survey_type=quest.survey_type_prefix_cd and  quest.fulfilment_vendor_nm='MEDALLIA'"""

  def decodeScore(respVal: String): String = {

    val score: String = respVal match {

      case LEXUS_NOT_RESPONDED => respVal
      case LEXUS_NA => respVal
      case LEXUS_EMPTY => LEXUS_NOT_RESPONDED

      case LEXUS_YES => LEXUS_100
      case LEXUS_NO => LEXUS_0

      case LEXUS_TRULY_OUTSTAND => LEXUS_100
      case LEXUS_VERY_GOOD => LEXUS_75
      case LEXUS_GOOD => LEXUS_50
      case LEXUS_FAIR => LEXUS_25
      case LEXUS_POOR => LEXUS_0

      case _ => if (respVal.isNumeric) (respVal.toInt * 10).toString() else respVal
    }

    score

  }

  def getScoreVal(ind1: Int, ind2: Int, qGrpNMList: List[Row]): Map[Int, String] = {
    val scoreR = qGrpNMList.filter(implicit row => (getStr(19) == LEXUS_SCORED))

    val retVal = if (!scoreR.isEmpty) { //19 score
      val scoreVal = getS(13)(scoreR.sortBy(f => (getS(15)(f).toInt, true)).head)
      val score = if (scoreVal == "") LEXUS_NOT_RESPONDED else scoreVal
      val colScore = decodeScore(score) //13 - response_txt 

      Map((ind1 -> score), (ind2 -> colScore))

    } else {
      Map((ind1 -> LEXUS_NOT_RESPONDED), (ind2 -> LEXUS_NOT_RESPONDED))
    }
    retVal
  }

  def getCommentVal(ind: Int, qGrpNMList: List[Row]): Map[Int, String] = {
    if (qGrpNMList.length > 0) {
      val resp = if (getS(13)(qGrpNMList.head) != "") getS(13)(qGrpNMList.head) else LEXUS_NOT_RESPONDED
      val retVal = Map((ind, resp))
      retVal
    } else {
      Map((0, LEXUS_NR))
    }
  }

  def getResponseVal(ind: Int, qGrpNMList: List[Row]): Map[Int, String] = {

    val scoreVal = qGrpNMList.sortBy(implicit f => (getS(15)(f), true)).map { qrow =>
      //Get the response Row with questionId that is the same as the questionId from questionRow and get the Response value from the Row
      val measure_type = getStr(19)(qrow)
      val respVal = if (measure_type == LEXUS_UNSCORED_TXT) getS(13)(qrow).replace(LEXUS_PIPELINE, LEXUS_TILDE)
      else getS(13)(qrow)
      respVal
    }
    val score = if (scoreVal.size > 0 && scoreVal.head != "") Map(ind -> scoreVal.head) else Map((ind, no_response))
    score
  }

  def getDealerResponseVal(ind: Int, qGrpNMList: List[Row]): Map[Int, String] = {
    val scoreVal = qGrpNMList.sortBy(implicit f => (getS(15)(f), true))
    val score = if ((scoreVal.size > 0) && (getS(13)(scoreVal.head) != "")) Map(ind -> getS(13)(scoreVal.head)) else Map((ind, LEXUS_YES))
    score
  }

  def getMultiVal(ind: Int, qGrpNMList: List[Row], fulfilmentVendor: String, surveyTypePrefix: String): Map[Int, String] = {
    val multiVal = qGrpNMList.filter(implicit row => ((getS(13) == LEXUS_YES) && (getS(15).toInt > 1))).map { qrow =>

      val questSeqNo = (getStr(12)(qrow)).replaceAll(LEXUS_PATH, LEXUS_DASH)
      val questSeqName = questconfig.getString((LEXUS_BRAND + fulfilmentVendor.toLowerCase + LEXUS_DOT + surveyTypePrefix.toLowerCase + LEXUS_QUEST_DOT + questSeqNo + LEXUS_DOT_NAME))
      val questSeqInc = questconfig.getBoolean((LEXUS_BRAND + fulfilmentVendor.toLowerCase + LEXUS_DOT + surveyTypePrefix.toLowerCase + LEXUS_QUEST_DOT + questSeqNo + LEXUS_DOT_INCL))

      val respMultiVal = (getS(13))(qrow)
      val questConfigVal = if (respMultiVal == LEXUS_YES && questSeqInc) questSeqName else LEXUS_EMPTY

      questConfigVal
    }

    val rspnsMultiVal =if(!multiVal.isEmpty) multiVal.filter(s => s != LEXUS_EMPTY).sortWith(_ < _).mkString(multiFieldDelimiter) else LEXUS_EMPTY 

    Map((ind, if (rspnsMultiVal == LEXUS_EMPTY) no_response else rspnsMultiVal))

  }

  def getScoreMultiVal(ind: Int, qGrpNMList: List[Row], fulfilmentVendor: String, surveyTypePrefix: String): Map[Int, String] = {
    val retVal = getResponseVal(ind, qGrpNMList) ++
      getMultiVal(ind + 1, qGrpNMList, fulfilmentVendor: String, surveyTypePrefix: String)
    retVal
  }

  def getResponseComment(ind: Int, questOrder: Int, qGrpNMList: List[Row], fulfilmentVendor: String = LEXUS_MED): Map[Int, String] = {

    val multiValComment = qGrpNMList.filter(implicit row => (getS(19) == "Text" || getS(14) == "OTHER_COMMENTS"))
    val comment = if (multiValComment.size > 0) getS(13)(multiValComment.head) else LEXUS_NOT_RESPONDED
    val text = if (comment == "") LEXUS_NOT_RESPONDED else comment
    Map(ind -> text)
  }

  def getScoreMultiValComment(ind: Int, questRespValOrder: Int, questRespMultiValOrder: Int, qGrpNMList: List[Row], fulfilmentVendor: String, surveyTypePrefix: String): Map[Int, String] = {

    //question level is not passed to scoreval, need to check if required? may be needed
    val retVal = getScoreVal(ind, ind + 1, qGrpNMList) ++
      getMultiVal(ind + 2, qGrpNMList, fulfilmentVendor, surveyTypePrefix) ++
      getResponseComment(ind + 3, questRespMultiValOrder, qGrpNMList)
    retVal

  }

  def getResponseMultiValComment(ind: Int, questRespValOrder: Int, questRespMultiValOrder: Int, qGrpNMList: List[Row], fulfilmentVendor: String, surveyTypePrefix: String): Map[Int, String] = {

    val retVal = getDealerResponseVal(ind, qGrpNMList) ++
      getMultiVal(ind + 1, qGrpNMList, fulfilmentVendor, surveyTypePrefix) ++
      getResponseComment(ind + 2, questRespMultiValOrder, qGrpNMList)
    retVal

  }

  def getResponseCommentVal(ind: Int, questRespValOrder: Int, questRespCommentOrder: Int, qGrpNMList: List[Row]): Map[Int, String] = {
    val sortedList = qGrpNMList.sortBy(implicit f => (getS(12).toInt, true))
    var rspnsRow = LEXUS_NOT_RESPONDED
    var rspnsRow2 = LEXUS_NOT_RESPONDED
    if (sortedList.size > 1) {
      rspnsRow = if (getS(13)(sortedList(0)) == "") LEXUS_NOT_RESPONDED else getS(13)(sortedList(0))
      rspnsRow2 = if (getS(13)(sortedList(1)) == "") LEXUS_NOT_RESPONDED else getS(13)(sortedList(1))
    } else if (sortedList.size > 0) {
      rspnsRow = if (getS(13)(sortedList(0)) == "") LEXUS_NOT_RESPONDED else getS(13)(sortedList(0))
    } else {
      rspnsRow
    }

    val retVal = Map((ind, rspnsRow), (ind + 1, rspnsRow2))
    retVal

  }

  def getMultiValComment(ind: Int, questRespValOrder: Int, questRespCommentOrder: Int, qGrpNMList: List[Row], fulfilmentVendor: String, surveyTypePrefix: String): Map[Int, String] = {
    val retVal = getMultiVal(ind, qGrpNMList, fulfilmentVendor, surveyTypePrefix) ++
      getResponseComment(ind + 1, questRespCommentOrder, qGrpNMList, fulfilmentVendor)
    retVal
  }

  def addEliteIndex(ind: Int, answerMap: Map[Int, String]): Map[Int, String] = {

    val valEliteReturnScore = answerMap.get(2).get
    val valEliteRecommendScore = answerMap.get(3).get

    val eliteIndex = (valEliteReturnScore, valEliteRecommendScore) match {
      case (LEXUS_NOT_RESPONDED | LEXUS_NA | LEXUS_EMPTY, LEXUS_NOT_RESPONDED | LEXUS_NA | LEXUS_EMPTY) => LEXUS_EMPTY
      case (valEliteReturnScore, (LEXUS_NOT_RESPONDED | LEXUS_NA | LEXUS_EMPTY)) => valEliteReturnScore
      case ((LEXUS_NOT_RESPONDED | LEXUS_NA | LEXUS_EMPTY), valEliteRecommendScore) => valEliteRecommendScore
      case (valEliteReturnScore, valEliteRecommendScore) => ((valEliteReturnScore.toInt + valEliteRecommendScore.toInt) / 2).toString
      case _ => LEXUS_NOT_RESPONDED
    }
    answerMap ++ Map((ind, eliteIndex))
  }

  def getMultiResponseVal(ind: Int, qGrpNMList: List[Row], questRespValOrder1: Int, questRespValOrder2: Int): Map[Int, String] = {
    val sortedList = qGrpNMList.sortBy(implicit f => (getS(15).toInt, true))
    val scoreVal = if (sortedList.size > 0) {
      if (getS(13)(sortedList(0)) == "") LEXUS_NOT_RESPONDED else getS(13)(sortedList(0))
    } else {
      LEXUS_NOT_RESPONDED
    }

    val unScoredVal = if (sortedList.size > 1) {
      if (getS(13)(sortedList(1)) == "") LEXUS_NOT_RESPONDED else getS(13)(sortedList(1))
    } else {
      LEXUS_NOT_RESPONDED
    }

    val retVal = Map((ind, scoreVal)) ++
      Map((ind + 1, unScoredVal))

    retVal

  }

  def getGroupValFillers(ind: Int, size: Int): Map[Int, String] = {

    val m = collection.mutable.Map.empty[Int, String]
    for (a <- ind to ind + size) m(a) = LEXUS_EMPTY
    m.toMap
  }

  def getMissingRespondedAnsFillers(start: Int, end: Int, ansMap: Map[Int, String]): Map[Int, String] = {
    val m = collection.mutable.Map.empty[Int, String]

    for (key <- start to end)
      if (ansMap.contains(key) == false) m(key) = LEXUS_NOT_RESPONDED
    m.toMap
  }

  def processSLSCPOAnswers(questionGroupNM: Map[String, List[org.apache.spark.sql.Row]], fulfilmentVendor: String, surveyTypePrefix: String) = {
    val colVal = questionGroupNM.map { implicit r =>
      val qGrpNM = r._1
      //println("qGrpNM" + qGrpNM)
      val qGrpNMList = r._2
      val ind = 9
      //val size = 28
      val myAnswers = qGrpNM match {
        //case "OSAT" | "ELITE_RETURN" | "ELITE_RECOMMEND" | "ELITE_COMMENT" => getCommonGroupVal(questionList, answerMap) //commented for now remove after CUSTPOC-1959 change 
        case "OSAT" => getScoreVal(4, 1, qGrpNMList)
        case "ELITE_RETURN" => getScoreVal(5, 2, qGrpNMList)
        case "ELITE_RECOMMEND" => getScoreVal(6, 3, qGrpNMList)
        case "ELITE_COMMENT" => getCommentVal(8, qGrpNMList)
        case "VEHICLE_REPLACEMENT" => getResponseCommentVal(ind, 1, 2, qGrpNMList)
        case "ATTRACTION" => getMultiValComment(ind + 2, 2, 2, qGrpNMList, fulfilmentVendor, surveyTypePrefix)
        case "WEBSITE_EXPERIENCE" => getResponseMultiValComment(ind + 4, 1, 3, qGrpNMList, fulfilmentVendor, surveyTypePrefix)
        case "CONSULT_EXPERIENCE" => getResponseMultiValComment(ind + 7, 1, 3, qGrpNMList, fulfilmentVendor, surveyTypePrefix)
        case "PAY_EXPERIENCE" => getResponseMultiValComment(ind + 10, 1, 3, qGrpNMList, fulfilmentVendor, surveyTypePrefix)
        case "FIN_MGR_EXPERIENCE" => getResponseMultiValComment(ind + 13, 1, 3, qGrpNMList, fulfilmentVendor, surveyTypePrefix)
        case "VEH_DELIVERY" => getMultiVal(ind + 16, qGrpNMList, fulfilmentVendor, surveyTypePrefix)
        case "VEH_DELIVERY_EXP" => getScoreMultiValComment(ind + 17, 1, 2, qGrpNMList, fulfilmentVendor, surveyTypePrefix)
        case "DLR_EXPERIENCE" => getCommentVal(ind + 21, qGrpNMList)
        case "VEHICLE_CONCERNS" => getScoreMultiVal(ind + 22, qGrpNMList, fulfilmentVendor, surveyTypePrefix)
        case "VEHICLE_CONCERNS_ASSIST_LTS" => getResponseVal(ind + 24, qGrpNMList)
        case "VEHICLE_CONCERNS_ASSIST_SC" => getResponseVal(ind + 25, qGrpNMList)
        case "VEHICLE_CONCERNS_ASSIST_SP" => getResponseVal(ind + 26, qGrpNMList)
        case "VEHICLE_CONCERNS_ASSIST_ODP" => getResponseVal(ind + 27, qGrpNMList)

        case _ => Map((0, LEXUS_NR))
      }
      myAnswers
    }
    val allAns = colVal.flatten.toMap
    getGroupValFillers(37, 21) ++ allAns ++ getMissingRespondedAnsFillers(1, 36, allAns)
  }

  def processSVCAnswers(questionGroupNM: Map[String, List[org.apache.spark.sql.Row]], fulfilmentVendor: String, surveyTypePrefix: String) = {
    val colVal = questionGroupNM.map { implicit r =>
      val qGrpNM = r._1

      val qGrpNMList = r._2
      val ind = 37
      val myAnswers = qGrpNM match {
        case "OSAT" => getScoreVal(4, 1, qGrpNMList) //Question 2, column indexes 4, 1
        case "ELITE_RETURN" => getScoreVal(5, 2, qGrpNMList) //Question 3, column indexes 5, 2
        case "ELITE_RECOMMEND" => getScoreVal(6, 3, qGrpNMList) //Question 3, column indexes 6, 3
        case "ELITE_COMMENT" => getCommentVal(8, qGrpNMList) //Question 3a, column indexes 7, Elite Comment
        case "SERVICE_WORK_TYPE" => getMultiValComment(ind, 2, 2, qGrpNMList, fulfilmentVendor, surveyTypePrefix) // Question 1, column indexes 37, 38, service_work_type, service_work_type_comment
        case "SERVICE_WRITEUP" => getResponseVal(ind + 2, qGrpNMList) // Question 4, column indexes 39 service_work_type
        case "SERVICE_EXPERIENCE" => getMultiValComment(ind + 3, 2, 2, qGrpNMList, fulfilmentVendor, surveyTypePrefix) // Question 4, column indexes 40, 41 service experience options and service experience comment
        //case "SERVICE_EXPERIENCE" => getScoreMultiValComment(ind + 2, 1, 2, questionList, answerMap, surveyTypePrefix) //service_experience_score, service_experience_options, service_experience_comment
        case "DEALERSHIP_SATISFACTION" => getMultiResponseVal(ind + 5, qGrpNMList, 1, 2) //Question 5, column index 42, 43, dealership_satisfaction_score, dealership_satisfaction_comment
        case "DEALER_COMMUNICATION_SATISFACTION" => getResponseMultiValComment(ind + 7, 1, 2, qGrpNMList, fulfilmentVendor, surveyTypePrefix) //Question 6, column index 44, 45, 46. dealer_communication_sat_score,dealer_communication_sat_options,dealer_communication_sat_comment 
        case "VEHICLE_FIX_FRIST_TIME" => getScoreMultiValComment(ind + 10, 1, 2, qGrpNMList, fulfilmentVendor, surveyTypePrefix) //Question 7, column index 47, 48, 49, 50. vehicle_fix_first_time_response,vehicle_fix_first_time_index, vehicle_fix_first_time_options,vehicle_fix_first_time_comment  
        case "SERVICE_DELIVERY_EXPERIENCE" => getScoreMultiValComment(ind + 14, 1, 2, qGrpNMList, fulfilmentVendor, surveyTypePrefix) //Question 8, column index 51, 52, 53, 54. service_delivery_experience_score,service_delivery_experience_index, service_delivery_experience_options,service_delivery_experience_comment  
        case "AFTER_SERVICE_SATISFACTION" => getMultiResponseVal(ind + 18, qGrpNMList, 1, 2) //Question 9, column index 55, 56. after_service_satisfaction_response, after_service_satisfaction_score
        case "DEALERSHIP_EXPERIENCE" => getResponseVal(ind + 20, qGrpNMList) //Question 10, column index 57. dealership_experience 
        case "DEALERSHIP_ENRICH" => getResponseVal(ind + 21, qGrpNMList) //Question 11, column index 58. dealership_enrich

        case _ => Map((0, LEXUS_NR))
        //case _ => getStr(grouping_nm_ind)(row) + " : " + getStr(rspns_txt)(rspnsRow.get) //for debugging purpose
      }
      /* addHeaderRow(headerRow) ++*/ myAnswers
    }
    val allAns = colVal.flatten.toMap
    getGroupValFillers(9, 27) ++ allAns ++ getMissingRespondedAnsFillers(37, 58, allAns) ++ getMissingRespondedAnsFillers(1, 8, allAns)
  }

  
}*/