import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object assignmentl2 {

  def findNa(data: DataFrame): Unit = {
    /*  null_counts = data.select([sqlf.count(sqlf.when(sqlf.col(c).isNull(), c)).alias(
      c) for c in data.columns]).collect()[0].asDict()
    col_to_drop = [k for k, v in null_counts.items() if v > 0]
   */
    //var s: String = ""
    val list = new ListBuffer[String]
    println("***print***")
    for (colname <- data.columns) {
      if (data.select(colname).collect().mkString(",").contains("NA")) {
        // s = s + colname
        list.append(colname)
      }
    }
    println(list)
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("assignment_l2").master("local[1]").getOrCreate()
    //val sc = spark.sparkContext
    /*
    Assignment1:
      a.	Use the log_file as input
    b.	Filter out records where package = “NA” and version = “NA”
    c.	Find total number of downloads for each package
    d.	Find Average download size for each Country
    */
    /*
    val raw_input = sc.textFile("src/main/data/log_file.txt")
    val data = raw_input.map(x=>x.split(","))
    println (data.foreach(x=> println(x(1).mkString)))
    // println(data.filter(x=> x(6).contains("NA") && x(7).contains("NA")).foreach(x=> print(x.mkString(","))))
    //data.foreach(x=> println(x.mkString(",")))
     */

    val data = spark.read.format("csv").option("header", "True").option("inferSchema", "True").load("spark-l1/src/main/data/log_file.txt")
    data.show()
    val f1 = data.select("*").where(""" package="NA" and version="NA"""")
    f1.show()
    val data1 = data.withColumn("package", when(col("package") === "NA", "null").otherwise(col("package")))
    data1.show()
    println("schema...")
    val schema = data.schema
    println(schema)
    val columns = data.columns
    println("columns.....")
    println(columns.mkString(","))
    println("Printing********")
    //read data column wise

    findNa(data1)
    println("Complete")


    //data1.na.fill("empty")
    //data1.show()
    val download_per_package = data.groupBy("package").agg(count("date"))
    download_per_package.show()

    val avg_download_country = data.groupBy("country").agg(round(avg("size"), 2))
    avg_download_country.show()

    excercise2and3(spark, data)

  }

  def excercise2and3(spark: SparkSession, data: DataFrame): Unit = {
    /*
    Assignment2:
    a.	Use the log_file as input
    b.	Create a Scala hashmap with country codes and country names for all country codes available in the log_file
    c.	Perform a broadcast join to generate the below output using RDD API
    Country Code, Country Name, Total Downloads
    d.	Save the output as a text file
    */
    val countries = data.select("country").distinct()
    println(countries.collect().mkString)
    //println(countries.)
    val h_map: Map[String, String] = Map("NZ" -> "New Zeland", "AU" -> "Australia", "GB" -> "Gulburg", "DE" -> "Denmark", "US" -> "United States", "FR" -> "France", "CH" -> "China", "DK" -> "Don't Know")
    data.na.replace("country", h_map).show()
    //above will create new dataset, to use updated one have to  store in new variable
    data.show()
    /*
    Spark SQL Assignments
    Assignment3:
    a.	Read the log_file as a RDD
    b.	Convert the input RDD to a Data Frame
    c.	Add a new column to Data Frame called Download_Type
    If Size < 100,000, Download_Type = “Small”
    If Size > 100,000 and Size < 1,000,000, Download_Type = “Medium”
    Else Download_Type = “Large”
    d.	Find the total number of Downloads by Country and Download Type
    e.	Save output as a Parquet File
    */
    val data_new = data.withColumn("Download_types", when(col("size") < 100000, "Small").when(col("size") > 100000 && col("size") < 1000000, "Medium").otherwise("Large"))
    data_new.show()

    val data_per_country_download = data_new.groupBy("country", "Download_types").agg(count("date")).orderBy("country")
    data_per_country_download.show()
  }

}
