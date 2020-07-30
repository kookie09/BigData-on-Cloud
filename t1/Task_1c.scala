// Load the input data and split each line into an array of strings
val vgdataLines = sc.textFile("hdfs:///user/ashhall1616/bdc_data/assignment/t1/vgsales-small.csv")
val vgdata = vgdataLines.map(_.split(";"))

// Task 1c:
// TODO: *** Put your solution here ***
val totalSales = vgdata.map(r => (r(3),((r(5).toDouble + r(6).toDouble + r(7).toDouble)))).reduceByKey(_+_)
val highestSales = totalSales.sortBy(_._2, false).take(1)
val lowestSales = totalSales.sortBy(_._2, true).take(1)
        println("Highest selling Genre: " + highestSales(0)._1 + "\tGlobal Sale (in millions): " + highestSales(0)._2 + "\nLowest selling Genre:" + lowestSales(0)._1 + "\tGlobal Sale (in millions):" + lowestSales(0)._2)


// Required to exit the spark-shell
sys.exit(0)