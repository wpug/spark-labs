package streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object GreatestTotalAmount extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(conf, Seconds(???)) //fill here

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

  val transformedStream = stream
//    .transform(???) //get only Kafka topic here using map on each RDD

  val mappedStream = transformedStream
  //transform each RDD
//      .transform(??? => ???
  //to split each row by comma (use regex): ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
//        ???
  //take only rows which size is equals 8 - we don't need incorrect data
//        ???
  //this is the data header:
  //PO_NUMBER,AGENCY_NAME,NIGP_DESCRIPTION,PO_TOTAL_AMOUNT,ORDER_DATE,SUPPLIER,FISCAL_YEAR,OBJECTID
  //we need only field number 2, 4 and fixed value 1 in format (String, (Double, Double))
//        .map(??? => (???, (???.toDouble, 1.0))))

  /*
    Leave only Agency Name and PO Total Amount

    Task should do the following:
    1. Find the maximum value in batch
    2. Calculate mean value by agency name in the window of 30 secs length with sliding interval equals 5

    Task should be printed separately.
   */

  val aggregationFunction: ((Double, Double), (Double, Double)) => (Double, Double) = ???

  //print only the biggest expense for each batch
//  mappedStream
  //sort descending and take only first value
  //use transform or foreachRDD to get access to RDD
//    ???
//    .print(???)

  //Part 2 - use reduceByKeyAndWindow to count sum of expenses and departments through the window. Remember to use aggregationFunction
  //In the next step count mean value
  //Print to std output
//  mappedStream
//    .reduceByKeyAndWindow(???)
//    .transform(???)
//    ???

  ssc.start()
  ssc.awaitTermination()

}
