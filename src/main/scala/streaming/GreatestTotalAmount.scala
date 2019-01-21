package streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GreatestTotalAmount extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(5))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "TotalExpensesAmount",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Set("expenses")

  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
  )

  case class Entry(agency: String, value: Double, count: Int)

  val transformedStream = stream
    .transform(
      kafkaRDD => kafkaRDD
        .map(kafkaRecord => kafkaRecord.value()))

  val castedStream = transformedStream
    .transform(
      rdd => rdd
        .map(row => row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
        .filter(array => array.length == 8)
        .map(splitted => (splitted(1), (splitted(3).toDouble, 1.0))))

  /*
    Leave only Agency Name and PO Total Amount
    Create new case class to cast

    Task should do the following:
    1. Find the maximum value in the window of 30 secs length with sliding interval equals 5
    2. Calculate mean value by agency name

    Task should be printed separately.
  */
  //  val aggregationFunction = ((xValue: Double, xCount: Int), (yValue: Double,  yCount: Int)) => ((xValue + yValue), (xCount + yCount))
  val aggregationFunction: ((Double, Double), (Double, Double)) => (Double, Double) = {
    case ((v1, w1), (v2, w2)) =>
      (v1 + v2, w1 + w2)
  }
  //  val aggregationFunction = {((xValue: Double, xCount: Int), (yValue: Double,  yCount: Int)) => ((xValue + yValue), (xCount + yCount))}
  //  val averageFunction: ((Double, Double), (Double, Double)) => (Double, Double) = {
  //    case ((v1, w1), (v2, w2)) =>
  //      ((v1 + v2) / (w1 + w2), w1)
  //  }


  castedStream.window(Seconds(15), Seconds(5))
    .transform(rdd => rdd.sortBy(x => x._2._1, ascending = false))

    .print(1)

  castedStream
    .window(Seconds(15), Seconds(5))
    .reduceByKeyAndWindow(aggregationFunction, Seconds(15), Seconds(5))
    .transform(rdd => rdd.map(x => (x._1, x._2._1 / x._2._2)))
    //    .reduceByKeyAndWindow(averageFunction, Seconds(15), Seconds(5))
    //    .foreachRDD(rdd => rdd.map(x => x._1)
    //    .reduceByKeyAndWindow()
    //    .transform(rdd=>rdd.map((agency,(sum, count))=>(agency, sum/count))
    //    .map((x,(y,z))=>(x,(y,z))
    //    .map((x,(sum, count)) =>(x,sum/count))
    //    .reduceByKeyAndWindow((x,y, z, c) => )

    .print()

  //    .foreachRDD(x => {
  //      println("New batch")
  //      x.collect().foreach(println)
  //    })


  ssc.start()
  ssc.awaitTermination()

}
