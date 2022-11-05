import org.apache.spark.sql.SparkSession

object assg_movie {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MOVIE_Analysis").master("local[1]").getOrCreate()
    val movie_data = spark.read.json("spark-l1/src/main/data/movies_en_1_0.json")
    movie_data.printSchema()
    val artists_data = spark.read.json("spark-l1/src/main/data/artists_en_1_2.json")
    artists_data.printSchema()
    /*
    Write spark SQL code to perform the following:
      a.	Read the files into data frames
      b.	Write a query to group titles of American movies by year
      c.	Display the first 5 records in command line
     */

    val command = "select year,count(title) from movie_data where country in ('USA') group by year "
    movie_data.createOrReplaceTempView("movie_data")
    spark.sql(command).show()
  }
}
