package core

import org.apache.spark.sql.SparkSession

object Partitioning extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Partitioning")
    .getOrCreate()

  val sc = spark.sparkContext

  //set
  val inputData = sc.parallelize(List(1,2,3,4,2,9,6,4,9,0,8,7,6,3,12,13,11,3,4,2,2,3,5,4,3,2,1,2,5,6,7,8,3,2,5,5,3,3,7), 4)

  val outputData = inputData.map(x=>(x,1))
    .reduceByKey(_+_)

  println("Original partitions:")
  outputData.foreachPartition(x => println(x.mkString(", ")))

  //Coalesce does not implicitly make shuffle, just move records from one partition to another.
  println("\nCoalesce:")
  outputData.coalesce(3).foreachPartition(x => println(x.mkString(", ")))

  //You can compare these partitions with original one. Repartition makes implicit shuffle.
  println("\nRepartition:")
  outputData.repartition(3).foreachPartition(x => println(x.mkString(", ")))

  //That won't work, there still will be 4 partitions
  println("\nIncrease num of partitions to 6 by coalesce:")
  outputData.coalesce(6).foreachPartition(x => println(x.mkString(", ")))

  //Increase partition number
  println("\nIncrease num of partitions to 6 by repartition:")
  outputData.repartition(6).foreachPartition(x => println(x.mkString(", ")))
}
