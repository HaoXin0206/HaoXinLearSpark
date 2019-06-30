package HaoXin.LearnSpark.SQL

import org.apache.spark.sql.SparkSession

object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("haoxin-sql")
      .master("local[6]")
      .getOrCreate()


    spark.sql(
      """
        |select
        |from table_name
        |where
      """.stripMargin)

  }

}
