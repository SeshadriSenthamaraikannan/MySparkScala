package com.toyota.SurveyCollection
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, SparkSession }
import com.toyota.SurveyCollection.pqssurvey




trait Comments { 
  
  case class all(a: String, b: String, c: String, d: String, e: String, f: String, g: String, h: String, i: String, j: String, k: String, l: String, m: String, n: String, o: String, p: String, q: String, r: String, s: String, t: String, u: String, v: String, w: String, x: String, y: String, z: String, aa: String, bb: String, cc: String, dd: String, ee: String, ff: String, gg: String, hh: String, ii: String, jj: String, kk: String, ll: String, mm: String, nn: String, oo: String, pp: String, qq: String, rr: String, ss: String, tt: String, uu: String, vv: String)
  case class surveyResponse(Problem_Category: String, Category_Name: String, Problem_Sub_Category: String, Sub_Category_Name: String, Code1: String, Comment: String, Condition: String, Responsibility: String, Problem_Severity: String, New_To_Toyota_Family: String, Comment_Control: String)
  case class surveyResponse_feature(Problem_Category: String, Category_Name: String, Problem_Sub_Category: String, Sub_Category_Name: String, Code1: String, Comment: String, Condition: String, New_To_Toyota_Family: String, QuesNbr: String)
  case class customer(Height: String, Weight: String, Riders: String, Gender: String, Age: String)
  case class vehicle(Vin: String, Model_number: String, Model_Year: String, Model_Name: String, Report_month: String, Plant: String, Engine: String, Line_off_date: String, Code2: String, Code3: String, Code4: String, Code5: String, Code6: String, Code7: String, Body_style: String, Count: String, Wave: String, Sale_Month: String, Region: String, District: String, Dealer: String, RDRDate: String, ProdYRMM: String, Tires: String, Audio_Unit: String, Mileage: String, Survey_Version: String, Katashiki: String, Report_Week: String, Exterior_Color: String, Interior_Color: String)
  case class vehicle_feature(Vin: String, Model_number: String, Model_Year: String, Model_Name: String, Report_month: String, Plant: String, Engine: String, Line_off_date: String, Code2: String, Code3: String, Code4: String, Code5: String, Code6: String, Code7: String, Body_style: String, Count: String, Wave: String, Sale_Month: String, Region: String, District: String, Dealer: String, RDRDate: String, ProdYRMM: String, Tires: String, Audio_Unit: String, Mileage: String, Survey_Version: String, Katashiki: String, Report_Week: String)
  case class audit(User: String, Create_ts: String, Update_ts: String)
  case class Survey(_id: String, SurveyID: String, SurveyType: String, Customer: customer, Vehicle: vehicle, SurveyResponse: Array[surveyResponse], Audit: audit)
  case class Survey_feature(_id: String, SurveyID: String, SurveyType: String, Customer: customer, Vehicle: vehicle_feature, SurveyResponse: Array[surveyResponse_feature], Audit: audit)
  
  
  
  case class all_pqsresp(a: String, b: String, c: String, d: String, e: String, f: String, g: String, h: String, i: String, j: String, k: String, l: String, m: String, n: String, o: String, p: String, q: String, r: String, s: String, t: String, u: String, v: String, w: String, x: String, y: String, z: String, aa: String, bb: String, cc: String, dd: String, ee: String, ff: String, gg: String, hh: String, ii: String, jj: String, kk: String, ll: String, mm: String, nn: String, oo: String)

  case class surveyResponse_pqsresp(Problem_Category: String, Problem_Sub_Category: String, Question_Response: String, Comments: String, Vehicle_Satisfaction: String, Value_Satisfaction: String, Dealer_Satisfaction: String, Quality_Satisfaction: String, Likely_to_Service: String, Likely_to_Recommend: String, Likely_to_Purchase: String, New_To_Toyota_Family: String, QuesNbr: String)
  case class customer_pqsresp(Height: String, Weight: String, Riders: String, Gender: String, Age: String, Cell_Phone: String, Cell_Provider: String,Customer_State: String, Customer_Zip: String )
  case class vehicle_pqsresp(Vin: String, Model_number: String, Model_Year: String, Model_Name: String,Report_Month: String, Plant: String, Engine: String, Line_off_date: String, Body_style: String, Wave: String, Sale_Month: String, Region: String, Tires: String, Audio: String, Mileage: String,Report_Week: String, Survey_Version: String, Katashiki: String)
  case class audit_pqsresp(User: String, Create_ts: String, Update_ts: String)
  case class Survey_pqsresp(_id: String, SurveyID: String, SurveyType: String, Customer: customer_pqsresp, Vehicle: vehicle_pqsresp, SurveyResponse: Array[surveyResponse_pqsresp], Audit: audit_pqsresp)

  
  
   def process(sparkSession:SparkSession):DataFrame
   def process1(sparkSession:SparkSession):DataFrame
   def process3(sparkSession:SparkSession):DataFrame
  

}