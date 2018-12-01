import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

object Q1 { 
   def main(args: Array[String]) {

      // Q1: Discover the schema of the input dataset and output it to a file.

   	// create a spark session
   	val spark = SparkSession.builder.appName("Q1").getOrCreate()

   	// read in the JSON into dataframe
   	val df = spark.read.json("../dataset.json")

   	// create a text path in same directory
   	val filePath = "./schema.txt"

      // print the schema to standard output
      df.printSchema()

   	// write the schema to the text path 
      new PrintWriter(filePath) { write(df.schema.treeString); close }
   } 
} 
