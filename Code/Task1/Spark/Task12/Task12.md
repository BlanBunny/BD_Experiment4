#spark-shell Task12

val log = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/input2/user_log_format1.csv")

val info = spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Task12/user_info_format1.csv")

val info1 = info.filter("age_range<4 and age_range>0").select("user_id","age_range")

val log1 = log.filter("time_stamp=1111 and action_type!=0").select("user_id","seller_id","action_type")

val dfjoin = info1.join(log1,"user_id")

val dfss = dfjoin.groupBy("seller_id").count()

//orderby count in desc and take first 100
val rddss = dfss.orderBy(dfss("count").desc).rdd.map(x=>(x(0),x(1))).take(100)

//save file
sc.parallelize(rddss).saveAsTextFile("hdfs://localhost:9000/Task12/output1-2")

:quit

hdfs dfs -cat /Task12/output1-2/*