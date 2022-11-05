import org.apache.spark.sql.SparkSession

object project2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("Project2").getOrCreate()
    // 1. Create RDD for above json file. // val data = sqlContext.read.json(path)
    val df = spark.read.json("spark-l1/src/main/data/test.json")
    // 2. display the contents in tabular format. //data.show or data.show()
    df.show()


    // 3. Apply filter on id , gender to restrict output. //val filter1 = data.select("id","gender")
    //filter1.show()

    val df1 = df.select("id", "gender")
    df1.show()
    //4. display contents in array format. // data.collect
    df.collect().foreach(x => println(x))
    //5. classify whole data into 2 files based on gender and save them as json files. // val part2 = data.repartition($"gender")
    // art2.write.format("json").mode("overwrite").save("/FileStore/tables/")

    /* df.write.format("json").mode("overwrite").partitionBy("gender").save("src/main/data/") */


    //6. differentiate between show and collect when used with data frames. // show -> tebular formate and collect -> Array format

    //7. sort the given data based out of gender and store in a file. // val sort_data = part2.sort($"gender")
    // sort_data.write.format("csv").save("/FileStore/tables/test")

    val sort_df = df.sort("gender")
    sort_df.show()

    //8. convert given json file into a text file. //data.write.format("text").save("/FileStore/tables/test")
  }
}
