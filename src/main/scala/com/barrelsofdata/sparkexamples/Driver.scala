package com.barrelsofdata.sparkexamples

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Driver {

  val JOB_NAME: String = "Structured Streaming Data Deduplication"
  val LOG: Logger = Logger.getLogger(this.getClass.getCanonicalName)

  def run(spark: SparkSession, kafkaBroker: String, kafkaTopic: String, fullOutputPath: String, deduplicatedOutputPath: String, windowSeconds: String): Unit = {
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val inputSchema: StructType = new StructType().add("ts", TimestampType).add("usr",StringType).add("tmp",IntegerType)

    val df:DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaBroker)
      .option("subscribe",kafkaTopic)
      .load()

    val data: Dataset[UserData] =  df
      .select(col("value").cast("STRING"))
      .select(from_json(col("value"), inputSchema).as("jsonConverted"))
      .select(col("jsonConverted.usr").as("user"), col("jsonConverted.tmp").as("temperature"), col("jsonConverted.ts").as("eventTime"))
      .as[UserData]

    val deduplicated: Dataset[UserData] = data
      .groupByKey(_.user)
      .flatMapGroupsWithState[UserData, UserData](OutputMode.Append(), GroupStateTimeout.NoTimeout)(StateOperations.deduplicate)

    val deduplicatedQuery: StreamingQuery = deduplicated
      .writeStream
      .format("parquet")
      .option("path", deduplicatedOutputPath)
      .trigger(Trigger.ProcessingTime(s"$windowSeconds seconds"))
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"${deduplicatedOutputPath}_checkpoint")
      .start()

    val fullQuery: StreamingQuery = data
      .writeStream
      .format("parquet")
      .option("path", fullOutputPath)
      .trigger(Trigger.ProcessingTime(s"$windowSeconds seconds"))
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"${fullOutputPath}_checkpoint")
      .start()

    deduplicatedQuery.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    if(args.length != 5) {
      println("Invalid usage")
      println("Usage: spark-submit --master <local|yarn> spark-structured-streaming-deduplication-1.0.jar <kafka_broker> <kafka_topic> <full_output_path> <deduplicated_output_path> <window_seconds>")
      LOG.error(s"Invalid number of arguments, arguments given: [${args.mkString(",")}]")
      System.exit(1)
    }
    val spark: SparkSession = SparkSession.builder().appName(JOB_NAME)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

    run(spark, args(0), args(1), args(2), args(3), args(4))

  }

}
