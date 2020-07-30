import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


// Define case classe for input data
case class Hashtag(tokenType: String, month: String, count: Int, hashtagName: String)
// Read the input data
val hashtags = spark.read.
  schema(Encoders.product[Hashtag].schema).
  option("delimiter", "\t").
  csv("hdfs:///user/ashhall1616/bdc_data/assignment/t2/twitter-small.tsv").
  as[Hashtag]



// Bonus task:
// TODO: *** Put your solution here ***
// NOTE: You only need to complete either the SQL *OR* RDD task to get the bonus marks

val hashtagNew = hashtags.withColumn("month",col("month").cast(IntegerType))// converting month type to int


val data =hashtagNew.drop($"tokenType")

val year1 = data.toDF("month1", "count1", "name1")
val year2 = data.toDF("month2", "count2", "name2")

val temp = year1.join(year2, (($"month1"-$"month2") === 1 || ($"month1"-$"month2") === 89) && ($"name1" === $"name2"))

val result = temp.map(f => (f.getAs[String]("name1"), f.getAs[Int]("month1"), f.getAs[Int]("count1"),  f.getAs[Int]("month2"), f.getAs[Int]("count2"),(f.getAs[Int]("count1")) - (f.getAs[Int]("count2")) )).orderBy($"_6".desc).first

printf("Hashtag name: " + result._1 + "\ncount of month " + result._4 + ": " + result._5 + "\ncount of month " + result._2 + ": " + result._3)

// Required to exit the spark-shell
sys.exit(0)
