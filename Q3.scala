import org.apache.spark.sql.SparkSession
import java.io._
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.Row

object Q3 { 
   def main(args: Array[String]) {

      // Q3: Identify the set of case officers (CASEOFFICER field)

   	// create a spark session
   	val spark = SparkSession.builder.appName("Q3").getOrCreate()

      // Implicit methods available in Scala for converting common Scala objects into DataFrames.
      import spark.implicits._

   	// read in the JSON into dataframe
   	val dataFrame = spark.read.json("../dataset.json")

      // create a text path in same directory
      val filePath = "./answer.txt"

      // select CASEOFFICER column
      var caseOfficerDF = dataFrame.select("CASEOFFICER")

      // remove duplicates and remove empty rows
      var caseOfficerDFWithoutDupes = caseOfficerDF.dropDuplicates().filter($"CASEOFFICER" !== "")
      
      // collect action
      var array = caseOfficerDFWithoutDupes.collect()

      // create a file writer
      val bw = new BufferedWriter(new FileWriter(filePath))

      // loop through array and write to file
      for (n <- array)
      {
         bw.write(n.toString + "\n")
      }

      bw.close()
   } 
} 
