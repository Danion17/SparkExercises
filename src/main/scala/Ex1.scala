import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro


object Ex1 {



  def exercise1()(implicit sparkSession: SparkSession) ={
    val data = sparkSession.read
      .format("avro")
      .load("src/main/resources/retail_db/products_avro/*")

    val output = data.filter(col("product_name").startsWith("Nike")
      && col("product_price") <= 23 && col("product_price") >= 20)
    output.write.format("parquet")
      .mode("overwrite")
      .option("compression","gzip")
      .save("src/main/resources/dataset/solution1/")
    output
  }

}
