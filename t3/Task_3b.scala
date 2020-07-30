import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)
case class VocabWord(vocabId: Int, word: String)

// Read the input data
val docwords = spark.read.
  schema(Encoders.product[Docword].schema).
  option("delimiter", " ").
  csv("hdfs:///user/ashhall1616/bdc_data/assignment/t3/docword-small.txt").
  as[Docword]
val vocab = spark.read.
  schema(Encoders.product[VocabWord].schema).
  option("delimiter", " ").
  csv("hdfs:///user/ashhall1616/bdc_data/assignment/t3/vocab-small.txt").
  as[VocabWord]


// Task 3b:
// TODO: *** Put your solution here ***
val temp = vocab.join(docwords, "vocabId").drop($"vocabId")

def firstLetter(x : String) : String = {
    x.substring(0,1)
}
val firstLetterUdf =
spark.udf.register[String, String]("firstLetter", firstLetter)


val data = temp.withColumn("firstLetter", firstLetterUdf($"word")).orderBy($"word".asc)

//data.write.mode("overwrite").parquet("file:///home/user191224219027/t3_docword_index_part.parquet")

data.write.mode("overwrite").partitionBy("firstLetter").parquet("file:///home/user191224219027/t3_docword_index_part.parquet")
//val test = spark.read.parquet("file:///home/user191224219027/t3_docword_index_part.parquet")

data.orderBy($"count".desc).show(10)

// Required to exit the spark-shell
sys.exit(0)
