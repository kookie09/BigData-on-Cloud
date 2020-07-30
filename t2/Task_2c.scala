// Load the input data and split each line into an array of strings
val twitterLines = sc.textFile("hdfs:///user/ashhall1616/bdc_data/assignment/t2/twitter-small.tsv")
val twitterdata = twitterLines.map(_.split("\t"))

// Task 2c:
// TODO: *** Put your solution here ***
// Remember that each month is a string formatted as YYYYMM
val x = scala.io.StdIn.readLine("x month: ")
val y = scala.io.StdIn.readLine("y month: ")

val tempX = twitterdata.filter(r => (r(1) == x)).map(r => (r(3), r(2)))
    
val tempY = twitterdata.filter(r => (r(1) == y)).map(r => (r(3), r(2)))

val temp = tempY.join(tempX).map(r => (r._1, r._2._1, r._2._2, (r._2._1.toInt - r._2._2.toInt))).sortBy((r => r._4), false).take(1)

println(s"x = $x, y = $y\n" + "hashtagName:" + temp(0)._1 +", countX: " + temp(0)._3 + ", countY: " + temp(0)._2)
// Required to exit the spark-shell
sys.exit(0)
