import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession._

object Q5 { 

   // function string to lower case
   val lower: String => String = _.toLowerCase

   // function to remove non alpha-numeric characters
   val removeNonAlphaNum : String => String = _.replaceAll("[^A-Za-z0-9 ]", "")
   

   def main(args: Array[String]) {

      // Q5: Count the occurrence of each word within the case text (CASETEXT field)
      // across all planning application records. 
      // Output each word and the corresponding count to a file.

   	// create a spark session
   	val spark = SparkSession.builder.appName("Q5").getOrCreate()

      // For implicit conversions like converting RDDs to DataFrames
      import spark.implicits._ 

   	// read in the JSON into dataframe
   	val dataFrame = spark.read.json("../dataset.json")

      // create a text path in same directory
      val filePath = "./answer"

      // convert functions to user defined function (UDF)
      val lowerUDF = udf(lower)
      val removeNonAlphaNumUDF = udf(removeNonAlphaNum)

      // $ is shorthand for the col() function
      // for the column CASETEXT, apply the user defined function
      var lowerCaseUDF = dataFrame.withColumn("CASETEXT", lowerUDF(($"CASETEXT")))
      var alphaNumericUDF = lowerCaseUDF.withColumn("CASETEXT", removeNonAlphaNumUDF(($"CASETEXT"))).select("CASETEXT")

      // split each line by a space
      // map each word to a count
      // reduction (reduceByKey transformation) (first transform to RDD)
      // convert back to DF for ease of use
      val counts = alphaNumericUDF.flatMap(line => line.toString.split(" "))
                 .map(word => (word, 1))
                 .rdd.reduceByKey(_ + _)
                 .toDF()

      // sort in descending order by value (i.e. count)
      var descendingOrder = counts.sort(desc("_2"))

      // bring the dataset onto the driver node.
      var repartitioned = descendingOrder.repartition(1)

      // convert repartitioned dataframe to rdd and then save this as text file
      repartitioned.rdd.saveAsTextFile(filePath)
   } 
} 
