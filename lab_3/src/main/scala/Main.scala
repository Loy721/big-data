import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .appName("Lab3")
      .master("local[2]")
      .getOrCreate()

    val langsDf = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/programming-languages.csv")
    val langsList = langsDf.select("name").rdd.map(_(0)).map(_.toString).map(_.toLowerCase).collect.toList

    val yearsPeriods = 2010 to 2020

    val posts = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .option("inferSchema", value = true)
      .load("data/posts_sample.xml")

    val creationsDateAndTags = posts.select("_Tags", "_CreationDate").where("_Tags is not null and _CreationDate is not null")
      .rdd.map(row => (row(0).toString.split(">"), row(1).toString.substring(0, 4).toInt))
      .flatMap {
        case (row) => row._1.map(tag => (row._2, tag.replace("<", "")))
      }.filter(row => langsList.contains(row._2))

    val res = yearsPeriods.map { reportYear =>
      creationsDateAndTags.filter {
          row => row._1 == reportYear
        }.map {
          row => (row._2, 1)
        }.reduceByKey(_ + _)
        .map(row => (reportYear, row._1, row._2))
        .sortBy(row => row._3, ascending = false)
        .collect.toList
    }.toList

    val reducedRes = res.reduce((a, b) => a.union(b))
    val resDf = spark.createDataFrame(reducedRes).select(col("_1").alias("Year"),
      col("_2").alias("Programming_language"),
      col("_3").alias("Count"))
    resDf.show()
    resDf.write.parquet("result")

    spark.stop()
  }
}
