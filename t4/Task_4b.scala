    import org.apache.spark.sql._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions._
    import spark.implicits._


    // Task 4b:
    // TODO: *** Put your solution here ***


    val data = spark.read.parquet("file:///home/user191224219027/t4_story_publishers.parquet")

    val x = data.toDF("storyId1", "publisher1")

    val y = data.toDF("storyId2", "publisher2")

    val result = x.join(y, ($"storyId1" === $"storyId2" && !($"publisher1" === $"publisher2") )).groupBy($"publisher1", $"publisher2").count().sort($"count".desc).filter($"publisher1" > $"publisher2")

result.collect.foreach(println)

// Required to exit the spark-shell
sys.exit(0)
