val log =spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/input2/user_log_format1.csv")

val info =spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/user_info_format1.csv")

val info1 = info.select("user_id","age_range").filter("age_range>0 and age_range<9")

val log1 = log.select("user_id","time_stamp","action_type").filter("time_stamp=1111 and action_type=2")

val dfjoin = info1.join(log1,"user_id")

val dfcount = dfjoin.groupBy("age_range").count()

dfcount.withColumn("ratio",dfcount("count")/dfjoin.count).show

/*将答案保存到本地*/
val dfratio = dfcount.withColumn("ratio",dfcount("count")/dfjoin.count).select("age_range","ratio").orderBy("age_range")

val rddr = dfratio.rdd.map(x=>(x(0),x(1)))

val rddr = dfratio.rdd.map(x=>(x(0),x(1))).collect()

sc.parallelize(rddr).saveAsTextFile("hdfs://localhost:9000/Task12/output2-2")
                                                                
