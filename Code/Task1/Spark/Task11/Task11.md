#spark-shell Task1-1

val data=sc.textFile("hdfs://localhost:9000/Task12/input2/user_log_format1.csv").flatMap(_.split("\n"))

val arr = data.take(1)

val data1 = data.filter(!arr.contains(_)).filter(line=>line.split(",")(5).equals("1111")).map{
            line => (line.split(",")(1), line.split(",")(6))
          }.mapValues(_.toInt)

val data2 = data1.filter(value => value._2>0)

val data3 = data2.countByKey().toSeq.sortWith(_._2>_._2).take(100)

val data4 = sc.parallelize(data3)


//save file
data4.saveAsTextFile("hdfs://localhost:9000/Task12/output1-1")

:quit

hdfs dfs -cat /Task12/output1-1/*