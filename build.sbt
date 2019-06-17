name := "SparkWorkshop"

version := "0.0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= {
  val sparkV = "2.4.3"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
  //"org.apache.bahir" %% "spark-streaming-twitter" % sparkV,
    "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkV
  )
}

