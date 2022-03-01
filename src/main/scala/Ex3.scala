import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex3 {
  def exercise3()(implicit sparkSession: SparkSession) ={

    val data = sparkSession.read.format("csv")
      .option("sep","\t")
      .load("src/main/resources/retail_db/customers-tab-delimited")

    val output = data.filter(col("_c1").startsWith("A"))
      .groupBy(col("_c7")).count()   //.agg(count("*").as("n_customer"))  // agg por .count("id)
      .filter(col("count") >= 50)
      .select(col("_c7").as("state"),col("count").as("customer_count"))


    output.write.format("parquet").option("compression","gzip")
      .mode("overwrite")
      .save("src/main/resources/dataset/solution3")

    output
  }
}
