# SparkLabs

### How to prepare to labs:
1. INSTALL DOCKER and docker-compose [ https://docs.docker.com/compose/ ]
2. cd docker, sh run.me <- that will pull the docker version of Kafka (credits to Spotify)
3. Download kafka (I was using kafka_2.11-2.0.0 version) and extract it somewhere
4. cd docker/docker-kafka/kafka and execute [ docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 spotify/kafka ]
Now you have working Kafka server on docker

To run Spark part, run in following order:
1. generator.sh
2. producer.sh ~/your/path/to/kafka/bin/folder
3. Spark Streaming job