package com.sandbox.src.main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._//{StringType, StructField, StructType, DateType}
import org.apache.commons.lang.StringUtils.substring
import java.text.SimpleDateFormat

import java.util.Properties
//import kafkashaded.org.apache.kafka.clients.producer._
import org.apache.spark.sql.ForeachWriter
import org.apache.kafka.clients.producer._
import org.apache.kafka.common._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.ml.feature.NGram
import java.sql.{Array=>SQLArray,_}
import org.apache.spark.ml.Pipeline

import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.types._

import scala.collection.mutable

object FileStreamExample {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("spark://18.205.181.166:7077")
      .appName("example")
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.conf.set("spark.sql.shuffle.partitions", 200)
    /////////////////////////////////////////////////////////////////////////////////////////////////////////    
 
    class  JDBCSink(url:String, user:String, pwd:String, dbase: String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
      val driver = "org.postgresql.Driver"
      var connection:Connection = _
      var statement:Statement = _
    
      def open(partitionId: Long,version: Long): Boolean = {
        Class.forName(driver)
        connection =java.sql.DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
      }

      def process(value: org.apache.spark.sql.Row): Unit = {
        statement.executeUpdate("INSERT INTO public." + dbase +"(name1,city_name1,state_init1,name2,city_name2,state_init2,distCol) " + "VALUES ('" + value(0) + "','" + value(1) + "','" + value(2) + "','" + value(3) +  "','" + value(4) +  "','" + value(5) +  "','" + value(6) +"');")
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close()
      }
    }
    
    class  JDBCSink2(url:String, user:String, pwd:String, dbase: String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
      val driver = "org.postgresql.Driver"
      var connection:Connection = _
      var statement:Statement = _

      def open(partitionId: Long,version: Long): Boolean = {
        Class.forName(driver)
        connection =java.sql.DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
      }

      def process(value: org.apache.spark.sql.Row): Unit = {
        statement.executeUpdate("INSERT INTO public." + dbase +"(name,city_name,state_init,zip_code) " + "VALUES ('" + value(0) + "','" + value(1) + "','" + value(2) + "','"+value(3)+"');")
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close()
      }
    } 
    /*def cityExistFunc: (String => Boolean) = (s => sbf.mightContain(s))
    def cityNoExistFunc: (String => Boolean) = (s => !sbf.mightContain(s))
    val myCityNoExistFunc = udf(cityNoExistFunc)
    val myCityExistFunc = udf(cityExistFunc)*/

    def dateFunc: (String => String) = {s => substring(s,4)}
    val myDateFunc = udf(dateFunc)
    def zipFunc: (String => String) = {s => substring(s,0,5)}
    val myZipFunc = udf(zipFunc)
    def nameFunc: (String => String) = {s => s.replace("'","''")}
    val myNameFunc = udf(nameFunc)
    def cityFunc: (String => String) = {s => s.replace("'","''").replace("ST. ", "SAINT ") }
    val myCityFunc = udf(cityFunc)

    //////////////////////////////////////////////////////////////////////////////////////////////////

    val schema = StructType(
      Array(StructField("CMTE_ID", StringType),
                StructField("AMNDT_IND", StringType),
                StructField("RPT_TP", StringType),
                StructField("TRANSACTION_PGI", StringType),
                StructField("IMAGE_NUM", StringType),
                StructField("TRANSACTION_TP", StringType),
                StructField("ENTITY_TP", StringType),
                StructField("NAME", StringType),
                StructField("CITY", StringType),
                StructField("STATE", StringType),
                StructField("ZIP_CODE", StringType),
                StructField("EMPLOYER", StringType),
                StructField("OCCUPATION", StringType),
                StructField("TRANSACTION_DT", StringType),
                StructField("TRANSACTION_AMT", StringType),
                StructField("OTHER_ID", StringType),
                StructField("TRAN_ID", StringType),
                StructField("FILE_NUM", StringType),
                StructField("MEMO_CD", StringType),
                StructField("MEMO_TEXT", StringType),
		StructField("SUB_ID", StringType)))

    /////////////////////////////////////////////////////////////////////////////////////////////////    
 
    /*val dfCity = sparkSession.read
         .format("csv")
         .option("header", "true") //reading the headers
         .option("mode", "DROPMALFORMED")
         .load("hdfs://ec2-18-205-181-166.compute-1.amazonaws.com:9000/user/uscitiesv14.csv")
    val dfCityUpper = dfCity.withColumn("city",upper($"city"))

    val dfCityUpperConcat = dfCityUpper.withColumn("cityState", concat($"city",lit(" "),$"state_id"))
    
    val expectedNumItems: Long = 10000 
    val fpp: Double = 0.005

    val sbf = dfCityUpperConcat.stat.bloomFilter($"cityState", expectedNumItems, fpp)*/

    /////////////////////////////////////////////////////////////////////////////////////////////////

    val dfGroundTruth = sparkSession.read
         .format("csv")
         .option("header", "true") //reading the headers
         .option("mode", "DROPMALFORMED")
         .load("hdfs://ec2-18-205-181-166.compute-1.amazonaws.com:9000/user/base_unique2.csv")

    val dfGroundTruthMod = dfGroundTruth
         .withColumn("NAME",myNameFunc(dfGroundTruth("NAME")))
         .withColumn("CITY",myCityFunc(dfGroundTruth("CITY")))
         .select("NAME","CITY","STATE","ZIP_CODE")
    
    def splitFunc: (String => Array[String]) = {s => s.split("")}
    val mySplitFunc = udf(splitFunc)
    def stringFunc: (mutable.WrappedArray[String] => String) = {s => s.mkString("").replace("   "," ").replace("  ", " ")}
    val myStringFunc = udf(stringFunc)

    val tokenizer = new RegexTokenizer().setPattern("").setInputCol("NAME").setMinTokenLength(1).setOutputCol("tokens")
    val ngram = new NGram().setN(3).setInputCol("tokens").setOutputCol("ngrams")
    val vectorizer = new HashingTF().setInputCol("ngrams").setOutputCol("features")
   
    val pipeline = new Pipeline().setStages(Array(tokenizer, ngram, vectorizer))
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)

    val model0 = pipeline.fit(dfGroundTruthMod)
    val vectorizedDf = model0.transform(dfGroundTruthMod).filter(isNoneZeroVector(col("features"))).select(col("NAME"),col("CITY"),col("STATE"),col("ZIP_CODE"), col("features"))

    val mh = new MinHashLSH().setNumHashTables(200).setInputCol("features").setOutputCol("hashValues")
    val model = mh.fit(vectorizedDf) 
    
    //////////////////////////////////////////////////////////////////////////////////////////////////

    //create stream from kafka topic data
    val fileStreamDf = sparkSession
	.readStream
	.format("kafka")
 	.option("kafka.bootstrap.servers", "18.205.181.166:9092")
 	.option("subscribe", "datatwo")
        .option("maxOffsetsPerTrigger",1000)
        .load()
        //.option("maxOffsetsPerTrigger",1000)
    
    val df_string = fileStreamDf.selectExpr("CAST(value AS STRING)")
    
    var df = df_string.select(from_json(col("value"),schema).alias("data")).select("data.*")

    //////////////////////////////////////////////////////////////////////////////////////////////////

   val dfFilter0 = df
      .filter(col("CMTE_ID")!=="")
      .filter(col("NAME")!=="")
      .filter(col("ZIP_CODE")!=="")
      .filter(col("TRANSACTION_DT")!=="")
      .filter(col("TRANSACTION_AMT")!=="")
      .filter(col("OTHER_ID")==="")

    val dfAlter1 = dfFilter0
      .withColumn("ZIP_CODE", myZipFunc(dfFilter0("ZIP_CODE")))
      .withColumn("YEAR", myDateFunc(dfFilter0("TRANSACTION_DT")).cast(IntegerType))
      .withColumn("TRANSACTION_AMT",dfFilter0("TRANSACTION_AMT").cast(IntegerType))
      .withColumn("NAME",myNameFunc(dfFilter0("NAME")))
      .withColumn("CITY",myCityFunc(dfFilter0("CITY")))

   val dfAlter2 = dfAlter1
      .filter(col("YEAR")<=2018)
      .filter(col("YEAR")>=1980)

   val dfAlter3 = dfAlter2
      .withColumn("timestamp",to_timestamp($"TRANSACTION_DT", "MMddyyyy"))
      .withColumn("TRANSACTION_DT",to_date($"TRANSACTION_DT", "MMddyyyy").alias("date"))
      .withColumn("CITYSTATE", concat($"CITY", lit(" "), $"STATE"))
      .withColumn("concatString",concat($"NAME"))
  
    /*dfFilter0.unpersist()
    dfAlter1.unpersist()
    dfAlter2.unpersist()*/  
 
    //val dfCityNoExists = dfAlter3
    //  .filter(myCityNoExistFunc($"CITYSTATE"))
    //val dfCityExists = dfAlter3
    //  .filter(myCityExistFunc($"CITYSTATE"))

    ///////////////////////////////////////////////////////////////////////////////////////////////////

    val vectorizedDf0 = model0.transform(dfAlter3).filter(isNoneZeroVector(col("features"))).select(col("NAME"),col("CITY"),col("STATE"),col("ZIP_CODE"), col("features"))
    
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    val url="jdbc:postgresql://postgresgpgsql.c0npzf7zoofq.us-east-1.rds.amazonaws.com:5432/postgresMVP"
    val user="gpgarner8324"
    val pwd="carmenniove84!"
    val writer = new JDBCSink(url, user, pwd, "potentialDuplicates")
    val writerUnique = new JDBCSink2(url, user, pwd, "uniqueContribs")
    
    val dbHashed = model.transform(vectorizedDf)
    val qHashed = model.transform(vectorizedDf0)

    val vectorizedQUERY = model.approxSimilarityJoin(dbHashed,qHashed, 0.3).filter("distCol != 0")

    val DF2 = vectorizedQUERY
      .withColumn("NAME_STREAM", col("datasetA.NAME"))
      .withColumn("CITY_STREAM", col("datasetA.CITY"))
      .withColumn("STATE_STREAM", col("datasetA.STATE"))
      .withColumn("NAME_CORPUS", col("datasetB.NAME"))
      .withColumn("CITY_CORPUS", col("datasetB.CITY"))
      .withColumn("STATE_CORPUS", col("datasetB.STATE"))
      .select("NAME_STREAM","CITY_STREAM","STATE_STREAM","NAME_CORPUS","CITY_CORPUS","STATE_CORPUS","distCol")
    val DF3 = DF2
      .withColumn("CITY_STREAM_SOUNDEX", soundex(col("CITY_STREAM")))
      .withColumn("CITY_CORPUS_SOUNDEX", soundex(col("CITY_CORPUS")))
      .withColumn("LEV_DIST", levenshtein(col("CITY_STREAM"),col("CITY_CORPUS")))
      .filter(col("LEV_DIST")<3)

    /*val query = DF3
      .writeStream
      .format("console")
      .outputMode("update")
      .start()
    query.awaitTermination()*/

    val query = DF3.select("NAME_STREAM","CITY_STREAM","STATE_STREAM","NAME_CORPUS","CITY_CORPUS","STATE_CORPUS","distCol") 
      .writeStream
      .foreach(writer)
      .outputMode("update")
      .option("checkpointLocation","hdfs://ec2-18-205-181-166.compute-1.amazonaws.com:9000/user/checkpoint0")
      .start()
    val query2 = dfAlter3.select("NAME","CITY","STATE","ZIP_CODE").distinct
      .writeStream
      .foreach(writerUnique)
      .outputMode("update")
      .option("checkpointLocation","hdfs://ec2-18-205-181-166.compute-1.amazonaws.com:9000/user/checkpoint1")
      .start()
    query.awaitTermination()
    query2.awaitTermination()
    /*val query0 = dfCityNoExists.select("NAME","CITY","STATE","ZIP_CODE")
      .writeStream
      .foreach(writerNo)
      .outputMode("update")
      .trigger(ProcessingTime("10 seconds"))
      .start()

    val query1 = dfCityExists.select("NAME","CITY","STATE","ZIP_CODE")
      .writeStream
      .foreach(writerYes)
      .outputMode("update")
      .trigger(ProcessingTime("25 seconds"))
      .start()*/
      
    //query0.awaitTermination()
  
  }
}
