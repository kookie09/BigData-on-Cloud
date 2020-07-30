import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


val docIds = scala.io.StdIn.readLine("Document IDs: ").split(" ")


// Task 3d:
// TODO: *** Put your solution here ***


val data = spark.read.parquet("file:///home/user191224219027/t3_docword_index_part.parquet")

for(docId <- docIds) {

    //val x = data.filter($"docId" === docId).drop($"firstLetter").sort($"count".desc)
        val x = data.filter($"docId" === docId).select($"docId", $"word", $"count").sort($"count".desc)
    if(x.count != 0)
    {
       println(x.first)
    }
}


// Required to exit the spark-shell
sys.exit(0)
