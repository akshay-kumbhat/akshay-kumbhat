import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object project1 extends Serializable {
  val log = Logger.getLogger(getClass.getName)
  //def rm_space (v:RDD[String],spark:SparkSession):RDD[String]={
  //  val sc = spark.sparkContext
  //  var z:RDD[String] = sc.emptyRDD[String]
  //if (v.toString.charAt(0)==' '){
  //    z += v.map(y => y.substring(1))
  //  }else{
  //    z+=v.toString
  //  }
  //  return z
  //}
  // case class as3  ( Property_ID: String, Location : String, Price : Double)

  def multiply(a: String)(b: String): Double = (a.toDouble * b.toDouble)

  def main(args: Array[String]): Unit = {

    val log = project1.log

    val spark = SparkSession.builder().appName("exercise1").master("local[1]").getOrCreate()

    val sc = spark.sparkContext

    val load: RDD[String] = sc.textFile("spark-l1/src/main/data/realestate.txt")

    // List the properties in Location = "Thomas County"
    val filter_rdd = load.filter(x => x.contains("Thomas County"))

    log.info(filter_rdd.foreach(x => println(x)))

    // Display the unique locations in command line
    val split_load = load.map(x => x.split("\\|"))
    val header = split_load.first()
    log.info(header.mkString(","))

    val split_no_header = split_load.filter(x => x(5) != "Size")
    log.info(split_load.count())
    log.info(split_no_header.count())
    /*  for (i <- split_no_header){
      log.info(i.mkString(","))
    }
  */
    log.info(split_no_header.foreach(x => println(x.mkString(","))))
    // val temp = rm_space(split_load.map(x =>  x(1),spark))

    val unique_location = split_no_header.map(x => x(1)).filter(x => x.charAt(0) != ' ').distinct()
    log.info(unique_location.foreach(x => println(x)))

    // Create a RDD having PropertyID, Location, Price (= size * Price SQ Ft) :

    val prop_loc_price = split_no_header.map(x => (x(0), x(1), multiply(x(5))(x(6))))
    log.info(prop_loc_price.foreach(x => println(x)))
    // Display the first 10 records in command line:
    log.info(prop_loc_price.take(10).foreach(x => println(x)))
    // Display count of Real estate properties for each location in command line:
    val total_properties = split_no_header.map(x => (x(1), 1)).reduceByKey((x, y) => x + y)
    log.info(total_properties.foreach(x => println(x)))

    // Create a RDD of Property IDs having 3 bedrooms:

    val $3_bedroom_properties = split_no_header.filter(x => x(3).toInt.equals(3))
    log.info($3_bedroom_properties.foreach(x => println(x.mkString(","))))
    log.info($3_bedroom_properties.count())

    // Create another RDD of Property IDs having at least 2 bathrooms:
    val $2_bathroom_atlest = split_no_header.filter(x => x(4).toInt >= 2)
    log.info($2_bathroom_atlest.foreach(x => println(x.mkString(","))))
    log.info($2_bathroom_atlest.count())
    // Create another RDD having 3 bedrooms and at least 2 bathrooms:

    val $3bed_and_atleast_2bath = split_no_header.filter(x => x(3).toInt.equals(4) && x(4).toInt >= 2)
    log.info($3bed_and_atleast_2bath.foreach(x => println(x.mkString(","))))
    log.info($3bed_and_atleast_2bath.count())
    // save the RDD to a file--> have to skip due to "HADOOP_HOME and hadoop.home.dir are unset."
    // $3bed_and_atleast_2bath.saveAsTextFile("src/main/output/Project2/")


    // Find the list of properties as per the below criteria and save it to a file
    //   i. Have 3 bedrooms
    //   ii. Have at least 2 bathrooms
    //   iii. Price <= 500000  (Price = size * Price SQ Ft)

    val final_result = split_no_header.filter(x => x(3).toInt.equals(3) && x(4).toInt >= 2 && multiply(x(5))(x(6)) <= 500000)
    log.info(final_result.foreach(x => println(x.mkString(","))))
    log.info(final_result.count())
    // Save the RDD to a fil
    // final_result.saveAsTextFile()
  }

}
