import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ex5 {

  def exercise5()(implicit sparkSession: SparkSession)={

    val data = sparkSession.read.format("parquet")
      .load("src/main/resources/retail_db/orders_parquet")


    val output = data.filter(col("order_status") === "COMPLETE")
      .select(col("order_id"),
        to_date(from_unixtime(col("order_date") / 1000,"dd-MM-YYYY"),"dd-MM-YYYY").as("order_date"),
        col("order_status"))
      .filter(year(col("order_date")) === lit(2014)&&
        ( month(col("order_date")) === lit(1)||
          month(col("order_date")) === lit(7) )
    )



    output.write.format("json")
      .mode("overwrite")
      .save("src/main/resources/dataset/solution5")

    output
  }
}
