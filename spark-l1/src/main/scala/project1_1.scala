import org.apache.spark.sql.SparkSession

object project1_1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Project1_1").master("local[1]").getOrCreate()
    val sc = spark.sparkContext
    //  a. Read the SampleData.txt as an RDD : val data = sc.textFile("file://<path>")
    val data = sc.textFile("spark-l1/src/main/data/sample.txt")

    //  b. Remove all punctuations marks from the text : val rmp = data.map(x=> x.replaceAll("[,.?;:\"()]","")
    val rm_punctuation = data.map { x =>
      x.replaceAll("[,.?;:()]", "")
    }

    rm_punctuation.foreach(x => println(x))
    //    c. Remove all numeric values : val rmnum = rmp.map(x=> x.replaceAll("[1234567890]","")
    val rm_num = rm_punctuation.map(x => x.replaceAll("[1234567890]", ""))
    rm_num.foreach(x => println())
    //    d. Find the total count of occurrences for each word :
    val word_count = rm_num.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    word_count.foreach(x => println(x))
    word_count.sortBy(_._1, true).foreach(x => println(x))
  }

}
