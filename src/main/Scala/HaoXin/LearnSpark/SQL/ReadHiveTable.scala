package HaoXin.LearnSpark.SQL

import org.apache.spark.sql._

object ReadHiveTable {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession
      .builder()
      .appName("ReadHiveTable")
      .master("local[6]")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .config("hive.metastore.uris", "thrift://haoxin01:9083")
      .config("spark.sql.adaptive.enabled","true")
      .config("spark.sql.adaptive.minNumPostShufflePartitions","10")
      .config("spark.sql.adaptive.maxNumPostShufflePartitions","800")
      .enableHiveSupport()
      .getOrCreate()
    builder.sql("show databases").show(false)
    builder.sql("use haoxin")
    builder.sql("show tables").show(false)
    builder.sql(
      """
        |select name,age,money,address
        |from userinfo
        |where address="宁波" and name="丁奕如"
      """.stripMargin)
      .show()

    builder.sql(
      """
        |select count(1) from userinfo
      """.stripMargin)
        .show()

    builder.sql(
      """
        |select *
        |from userinfo
        |where name="周娜"
      """.stripMargin)
        .show()


    builder.sql(
      """
        |select * from userinfo
        |where name="申佳伟"
      """.stripMargin)
      .show()

    builder.sql(
      """
        |select * from userinfo
        |where name="王龙"
      """.stripMargin)
      .show()

    builder.sql(
      """
        |select * from userinfo
        |where name="丁秀伟"
      """.stripMargin)
      .show()

    builder.sql(
      """
        |select * from userinfo
        |where name="张包财"
      """.stripMargin)
      .show()

    builder.sql(
      """
        |select * from userinfo
        |where name="周周"
      """.stripMargin)
      .show()

    builder.sql(
      """
        |select distinct name from userinfo
      """.stripMargin)
      .show(false)

    builder.sql(
      """
        |select name from userinfo
      """.stripMargin)
      .rdd
      .map(a=>(a.getString(0),1))
      .reduceByKey(_+_)
      .map(a=>a.swap)
      .sortByKey()
      .map(a=>a.swap)
      .top(20)
      .foreach(println)

    Thread.sleep(1000000)

  }

}
