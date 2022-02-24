import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object Ex2 {
  def exercise2()(implicit sparkSession: SparkSession) ={



    val data = sparkSession.read
      .csv("src/main/resources/retail_db/categories")

    val output = data.orderBy(desc("_c0")).select(col("_c0"),col("_c2"))

    output.write.format("csv").mode("overwrite")
      .option("header",true).option("sep",":")
      .save("src/main/resources/dataset/solution2")

    output
  }
}
