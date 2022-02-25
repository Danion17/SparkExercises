import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex4 {
  def exercise4()(implicit sparkSession: SparkSession)={
    val data = sparkSession.read
      .csv("src/main/resources/retail_db/categories")

    val output = data.filter(col("_c2") === "Soccer")
      .select(col("_c0").as("id"),col("_c1").as("department_id"),col("_c2").as("name"))

    output.write.format("csv").option("sep"," ")
      .mode("overwrite")
      .save("src/main/resources/dataset/solution4")
    output
  }
}
