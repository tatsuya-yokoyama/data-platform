# data-platform

## Kafka

起動 with Docker
```shell
cd ~/Documents/git/data-platform/docker
docker compose up -d
```

Kafdrop
http://localhost:9000/

停止
```shell
docker compose down
```

producer
```shell
cd ~/Documents/git/data-platform/kafka-producer-sample
mvn celan compile
mvn exec:java -Dexec.mainClass="com.yokoyama.Producer"
```

consumer
```shell
cd ~/Documents/git/data-platform/kafka-consumer-sample
mvn celan compile
mvn exec:java -Dexec.mainClass="com.yokoyama.Consumer"
```