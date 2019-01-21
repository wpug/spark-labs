KAFKA_BIN=$1
tail -n +1 -f expenses | sh $KAFKA_BIN/kafka-console-producer.sh --broker-list localhost:9092 --topic expenses