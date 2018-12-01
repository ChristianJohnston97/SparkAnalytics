import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.Row

object Q2 { 
   def main(args: Array[String]) {

      // Q2: What is the total number of planning application records in the dataset?

   	// create a spark session
   	val spark = SparkSession.builder.appName("Q2").getOrCreate()

   	// read in the JSON into dataframe 
   	val df = spark.read.json("../dataset.json")

      // convert dataframe operator into an RDD & count the number of rows (i.e. total number of application records)
      val number = df.rdd.count()

      // create a text path in same directory
      val filePath = "./answer.txt"

      // write the anwer to the file path 
      new PrintWriter(filePath) { write(number.toString); close }

      // standard output as well 
      println(number);

   } 
} 
