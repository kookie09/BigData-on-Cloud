// Load the input data and split each line into an array of strings
val vgdataLines = sc.textFile("hdfs:///user/ashhall1616/bdc_data/assignment/t1/vgsales-small.csv")
val vgdata = vgdataLines.map(_.split(";"))


// Task 1d:
// TODO: *** Put your solution here ***
val temp = vgdata.map(r => (r(4), 1 )).reduceByKey(_+_)
val dataCount =  vgdata.count

val data = temp.map(r => (r._1, r._2, (r._2*100).toDouble/dataCount)).sortBy((x => (x._3)), false).take(50)
data.foreach(println)

// Required to exit the spark-shell
sys.exit(0)
