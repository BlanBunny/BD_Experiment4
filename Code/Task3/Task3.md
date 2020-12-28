//terminal 
cd $SPARK_HOME 
bin/spark-shell 



//scala 
val user_log = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/input2/user_log_format1.csv") 
val user_info = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/user_info_format1.csv") 

user_log.createOrReplaceTempView("table_log") 
user_info.createOrReplaceTempView("table_info") 



//3-1 
val people1=spark.sql("select gender,count(*) as num from (select distinct a.user_id,gender from table_log a,table_info b where a.user_id=b.user_id and a.action_type='2' and gender in('0','1')) group by gender") 

People1.show 

// //下面显示ratio(但不是sql语句 

People1.agg("num"-> "sum").show //结果为407308 

People1.withColumn("ratio",People1("num")/407308).show 

 

 

//3-2 
val people2=spark.sql("select age_range,count(*) as num from (select distinct a.user_id,age_range from table_log a,table_info b where a.user_id=b.user_id and a.action_type='2' and age_range in('1','2','3','4','5','6','7','8')) group by age_range order by age_range") 

people2.show 

// //下面显示ratio(但不是sql语句 

People2.agg("num"-> "sum").show //结果为329039 

People2.withColumn("ratio",People2("num")/329039).show