#!/bin/sh
docker exec -it kafka-mpmc /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic business-events --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1
docker exec -it kafka-mpmc /opt/kafka/bin/kafka-topics.sh --describe --topic business-events --bootstrap-server localhost:29092
