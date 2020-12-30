val log =spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/input2/user_log_format1.csv")

val info =spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/user_info_format1.csv")

val log1 = log.select("user_id","time_stamp","action_type").filter("time_stamp=1111 and action_type=2")

val info1 = info.select("user_id","gender").filter("gender=1 or gender=0")

val dfjoin1 = info1.join(log1,"user_id")

val dfcount1 = dfjoin1.groupBy("gender").count()

dfcount1.withColumn("ratio",dfcount1("count")/dfjoin1.count).show

val rddr1 = dfcount1.withColumn("ratio",dfcount1("count")/dfjoin1.count).select("gender","ratio").rdd.map(x=>(x(0),x(1))).collect()

sc.parallelize(rddr1).saveAsTextFile("hdfs://localhost:9000/Task12/output2-1")