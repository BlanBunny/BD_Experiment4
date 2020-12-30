/terminal 
cd $SPARK_HOME 
bin/spark-shell 
 
 
//scala 
val user_log = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/input2/user_log_format1.csv") 
 
val user_info = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/user_info_format1.csv") 
 
user_log.createOrReplaceTempView("table_log") 
 
user_info.createOrReplaceTempView("table_info") 
 
 
//3-1 
val people1=spark.sql("select gender,count(*) as num from (select distinct a.user_id,gender from table_log a,table_info b where a.user_id=b.user_id and a.action_type='2' and gender in('0','1')) group by gender") 
people1.show 
//下面显示ratio
people1.agg("num"-> "sum").show //结果为407308 
people1.withColumn("ratio",people1("num")/407308).show 