import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._

object Ex6 {

    def exercise6()(implicit sparkSession: SparkSession)={
      val data  = sparkSession.read.format("avro")
        .load("src/main/resources/retail_db/customers-avro")

      val output = data.withColumn("customer_name",
        concat(substring(col("customer_fname"),0,1)
          ,lit(" ")
          ,col("customer_lname")))
        .select("customer_id","customer_name")

      output.write.format("csv")
        .option("sep","\t")
        .option("compression","bzip2")
        .mode("overwrite")
        .save("src/main/resources/dataset/solution6")

      output

    }
}
