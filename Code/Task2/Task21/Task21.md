//2-1 双十一购买了商品的男女比例


val df_log =spark.read.format("csv").option("header","true").load("hdfs:/localhost:9000/Task12/input2/user_log_format1.csv")

val df_info =spark.read.format("csv").option("header","true").load("hdfs:/localhost:9000/Task12/user_info_format1.csv")

val df_log1 = df_log.select("user_id","time_stamp","action_type").filter("time_stamp=1111 and action_type=2")

val df_info1 = df_info.select("user_id","gender").filter("gender=1 or gender=0")

val dfjoin = df_info1.join(df_log1,"user_id")

val dfcount = dfjoin.groupBy("gender").count()

dfcount.withColumn("ratio",dfcount("count")/dfjoin.count).show

+------+------+------------------+                                              
|gender| count|             ratio|
+------+------+------------------+
|     0|846054|0.7232596926427983|
|     1|323725|0.2767403073572017|
+------+------+------------------+



val rddrg = dfcountg.withColumn("ratio",dfcount("count")/dfjoin.count).select("gender","ratio").rdd.map(x=>(x(0),x(1))).collect()

sc.parallelize(rddrg).saveAsTextFile("hdfs://localhost:9000/Task2/output2-1")