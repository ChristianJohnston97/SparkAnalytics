import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession._
import scala.math._

object Q6 { 

   def main(args: Array[String]) {

      // Q6: Measure the average public consultation duration in DAYS 
      // (i.e. diff between PUBLICCONSULTATIONENDDATE and PUBLICCONSULTATIONSTARTDATE 

   	// create a spark session
   	val spark = SparkSession.builder.appName("Q6").getOrCreate()

      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._ 

   	// read in the JSON into dataframe
   	val dataFrame = spark.read.json("../dataset.json")

      // select the two columns needed
      var newDF = dataFrame.select("PUBLICCONSULTATIONENDDATE", "PUBLICCONSULTATIONSTARTDATE")

      // difference between two unix_timestamps in seconds
      var seconds = newDF.withColumn("date_diff_seconds", (unix_timestamp($"PUBLICCONSULTATIONENDDATE", "dd/MM/yyyy") - unix_timestamp($"PUBLICCONSULTATIONSTARTDATE", "dd/MM/yyyy")))

      // convert to days
      // 60*60*24 = 86,400
      var daysDF = seconds.withColumn("days", $"date_diff_seconds" / 86400)

      // calculate the average value
      var avgValDF = daysDF.select(avg($"days"))

      // create a text path in same directory
      val filePath = "./answer"

      // bring the dataset onto the driver node.
      var repartitioned = avgValDF.repartition(1)

      // convert repartitioned dataframe to rdd and then save this as text file
      repartitioned.rdd.saveAsTextFile(filePath)
   } 
} 
