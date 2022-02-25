import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App{

  implicit val sparkSession = SparkSession.builder()
    .master(master = "local")
    .appName(name = "curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._

  println("Exercise 1:")
  val results1 = Ex1.exercise1()
  results1.show()

  println("Exercise 2:")
  val results2 = Ex2.exercise2()
  results2.show()

  println("Exercise 3:")
  val results3 = Ex3.exercise3()
  results3.show()

  println("Exercise 4:")
  val results4 = Ex4.exercise4()
  results4.show()

  println("Exercise 5:")
  val results5 = Ex5.exercise5()
  results5.show()

  println("Exercise 6:")
  val results6 = Ex6.exercise6()
  results6.show()


}



