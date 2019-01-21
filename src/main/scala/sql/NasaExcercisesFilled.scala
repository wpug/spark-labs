package sql

import org.apache.spark.sql.SparkSession


/*
The purpose of this is to count from which website number of the successful connections was the biggest (referer=200).
Use Dataframe API.

  1. Read file nasa_19950801.tsv (separator `t`, header in first line)
  2. Filter responses other than 200
  3. Calculate remaining requests
  4. Output should be stored as one textfile in folder called output and should contain hostname and number of requests ordered descending.
 */
object NasaExcercisesFilled extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Partitioning")
    .getOrCreate()

  val sqlContext = spark.sqlContext

  //0. import sqlContext.implicits._
  import sqlContext.implicits._

  //1. Read data from file using sqlContext. Remember to read header from file and set custom delimiter.
  val rawData = sqlContext.read.format("com.databricks.spark.csv")
    .option("delimiter","\t")
    .option("header","true")
    .load("./nasa_19950801.tsv")

  //2. Filter requests
  val filteredData = rawData.filter(rawData.col("response") === 200)

  //3. Reduce by address and calculate quantity
  val reducedData = filteredData.groupBy("host")
    .count()
    .coalesce(1)
    .orderBy($"count".desc)

  //4. Save output as textfile (output should be tab separated and sorted descending).
  reducedData.write.option("delimiter","\t").option("header","true").csv("output")
}
