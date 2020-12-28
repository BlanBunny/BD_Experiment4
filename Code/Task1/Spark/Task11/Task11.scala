import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Task11 {
  def main(args: Array[String]): Unit ={
   /*
   if(args.length<2){
      System.err.println("Usage: <num> <output path>") //all fixed
      System.exit()
    }
    */

    val conf = new SparkConf().setAppName(Scala_Task11)
    val sc = new SparkContext(conf)
    //val df = spark.read.format("csv").option("header","true").load("hdfs:/localhost:9000/Task12/input2/user_log_format1.csv")
    val df = sc.textFile("hdfs:/localhost:9000/Task12/input2/user_log_format1.csv").flatMap(_.split("\n"))
    //ignore the first line
    val arr = df.take(1)
    val df1 = df.filter(!arr.contains(_)).filter(line=>line.split(",")(5).equals("1111")).map{
      line => (line.split(",")(1), line.split(",")(6))
    }.mapValues(_toInt)
    //action_type handle
    val df2 = df1.filter(value => value._2>0)
    val df3 = df2.countByKey().toSeq.sortWith(_._2>_._2).take(100)       //choose Top 100
    //seq -> rdd -> textfile
    val df4 = sc.parallelize(df3)
    df4.saveAsTextFile("hdfs:/localhost:9000/Task12/output1-1")    //output path
    sc.stop()
  }
}
