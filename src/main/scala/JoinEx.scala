import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object JoinEx {


  def joinExercise()(implicit sparkSession: SparkSession)={

    import sparkSession.implicits._

    val customers = sparkSession.read.csv("src/main/resources/retail_db/customers")
      .withColumnRenamed("_c0","customer_id")


    val orders = sparkSession.read.csv("src/main/resources/retail_db/orders")
      .withColumnRenamed("_c2","customer_id")



    val output = orders.groupBy("customer_id").count().filter($"count" > 5)
      .join(customers.filter($"_c1".startsWith("M")),"customer_id")
      .orderBy(desc("count"))
      .select($"_c1".as("customer_fname"),$"_c2".as("customer_lname"),$"count")

    output.write
      .mode("overwrite")
      .option("compression","gzip")
      .option("sep","|")
      .csv("src/main/resources/dataset/solutionJoin")



    output

  }
}
