import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object Q4 { 
   def main(args: Array[String]) {

      // Q4: Who are the top N agents (AGENT field) submitting the most number of applications? 
      // Allow N to be configurable and output the list to a file.

   	// create a spark session
   	val spark = SparkSession.builder.appName("Q4").getOrCreate()

      // first and only argument is n
      var n = args(0).toInt

      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._

   	// read in the JSON into dataframe
   	val dataFrame = spark.read.json("../dataset.json")

      // create a text path in same directory
      val filePath = "./answer"

      // select AGENT column where agent does not equal null
      var agents= dataFrame.select("AGENT").filter($"AGENT" !== "")

      // count the number of applications for each agent
      var numberOfApplications = agents.groupBy("AGENT").count()

      // order by count in descending order 
      var descendingOrder = numberOfApplications.sort(desc("count"))

      // get the top N using command line arguments
      var topN = descendingOrder.limit(n)

      // finally get just the AGENT names back
      var finalDataFrame = topN.select("AGENT")

      // bring the dataset onto the driver node.
      var repartitioned = finalDataFrame.repartition(1)

      // convert repartitioned dataframe to rdd and then save this as text file
      repartitioned.rdd.saveAsTextFile(filePath)
   } 
} 
