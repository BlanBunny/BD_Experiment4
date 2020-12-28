import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Task12 {
 /*
  def main(args: Array[String]): Unit ={
    if(args.length<2){
      System.err.println("Usage: <num> <output path>") //all fixed
      System.exit()
    }
  */

    val conf = new SparkConf().setAppName(Scala_Task12)
    val sc = new SparkContext(conf)
  // read in csv as dataframe
    val df_log = spark.read.format("csv").option("header","true").load("hdfs:/localhost:9000/Task12/input2/user_log_format1.csv")
    val df_info = spark.read.format("csv").option("header","true").load("hdfs:/localhost:9000/Task12/user_info_format1.csv")
  //clean data
    val df_info1 = df_info.filter("age_range<4 and age_range>0").select("user_id","age_range")
   `val df_log1 = df_log.filter("time_stamp=1111 and action_type!=0").select("user_id","seller_id","action_type")
  //join by user_id
    val dfjoin = df_info1.join(df_log1,"user_id")
  //count by seller_id
    val df_count = dfjoin.groupBy("seller_id").count()
  //orderby count in desc and take Top 100
    val rddss = df_count.orderBy(df_count("count").desc).rdd.map(x=>(x(0),x(1))).take(100)
  //save as testFile
    sc.parallelize(rddss).saveAsTextFile("hdfs:///e04/output1-2")
  sc.stop()
  }
}
