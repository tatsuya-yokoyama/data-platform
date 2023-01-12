# data-platform

## Kafka
Start with Docker
```shell
cd data-platform/docker
docker compose up -d
```

Kafdrop
http://localhost:9000/

Stop
```shell
docker compose down
```

## Event producer
```shell
cd kafka-producer-sample
mvn celan compile
mvn exec:java -Dexec.mainClass="com.yokoyama.Producer"
```

## Flink Job
```shell
cd kafka-consumer-sample
mvn celan compile
mvn exec:java -Dexec.mainClass="com.yokoyama.FlinkJob"
```

## ClickHouse
can use clickhouse provided by [smart-ad-campaign-metrics](https://github.com/smartnews/smart-ad-campaign-metrics)