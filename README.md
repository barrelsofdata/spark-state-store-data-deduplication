# Spark Structured Streaming Data Deduplication using State Store
This is a project showing how the spark in-built HDFS backed state store can be used to deduplicate data in a stream. The related blog post can be found at [https://www.barrelsofdata.com/data-deduplication-spark-state-store](https://www.barrelsofdata.com/data-deduplication-spark-state-store)

## Build instructions
From the root of the project execute the below commands
- To clear all compiled classes, build and log directories
```shell script
./gradlew clean
```
- To run tests
```shell script
./gradlew test
```
- To build jar
```shell script
./gradlew shadowJar
```
- All combined
```shell script
./gradlew clean test shadowJar
```

## Run
Ensure your local hadoop cluster is running ([hadoop cluster tutorial](https://www.barrelsofdata.com/apache-hadoop-pseudo-distributed-mode)) and start two kafka brokers ([kafka tutorial](https://www.barrelsofdata.com/apache-kafka-setup)).
- Create kafka topic
```shell script
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 2 --partitions 2 --topic streaming-data
```
- Start streaming job
```shell script
spark-submit --master yarn --deploy-mode cluster build/libs/spark-state-store-data-deduplication-1.0.jar <KAFKA_BROKER> <KAFKA_TOPIC> <FULL_OUTPUT_PATH> <DEDUPLICATED_OUTPUT_PATH> <WINDOW_SIZE_SECONDS>
Example: spark-submit --master yarn --deploy-mode client build/libs/spark-state-store-data-deduplication-1.0.jar localhost:9092 streaming-data fullOutput deduplicatedOutput 5
```
- You can feed simulated data to the kafka topic
- Open new terminal and run the shell script located at src/test/resources/dataProducer.sh
- Produces the two instances of the following json structure every 1 second: {"ts":1594307307,"usr":"user1","tmp":98}
```shell script
cd src/test/resources
./dataProducer.sh localhost:9092 streaming-data
```

### View Results
Open a spark-shell and use the following code, do change the paths to where the outputs are stored.
```scala
spark.read.parquet("fullOutput").orderBy("user","eventTime").show(truncate = false)

spark.read.parquet("deduplicatedOutput").orderBy("user","eventTime").show(truncate = false)
```