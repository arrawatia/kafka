```

./gradlew releaseTarGz -x signArchives

0278* cd core/build/distributions
10279* ls
10280* tar -xvf kafka_2.10-0.10.1.0-SNAPSHOT.tgz
10281* cd kafka_2.10-0.10.1.0-SNAPSHOT
10283* bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
10285* bin/kafka-server-start.sh config/server.properties

Run the KafkaProxy class (add conf/proxy.properties as args in intellij)

kafkacat -L -b localhost:19092

Metadata for all topics (from broker -1: localhost:19092/bootstrap):
 1 brokers:
  broker 0 at 192.168.0.101:9092
 -65536 topics:

```
