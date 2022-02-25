import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex4 {
  def exercise4()(implicit sparkSession: SparkSession)={
    val data = sparkSession.read
      .csv("src/main/resources/retail_db/categories")

    val output = data.filter(col("_c2") === "Soccer")

    output.write.format("csv").option("sep"," ")
      .mode("overwrite")
      .save("src/main/resources/dataset/solution4")
    output
  }
}
