import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import spark.implicits._


    val queryWords = scala.io.StdIn.readLine("Query words: ").split(" ")


// Task 3c:
// TODO: *** Put your solution here ***
val data = spark.read.parquet("file:///home/user191224219027/t3_docword_index_part.parquet")


for(queryWord <- queryWords) {
    
   // val x = data.filter($"firstLetter" === queryWord.substring(0,1)).filter($"word" === queryWord).sort($"count".desc).select($"word", $"docId").where($"word" === queryWord).first

    val result = data.filter($"firstLetter" === queryWord.substring(0,1)).where($"word" === queryWord).sort($"count".desc).select($"word", $"docId")
    if (result.count != 0)
    {
        println(result.first)
    }
}





// Required to exit the spark-shell
sys.exit(0)
