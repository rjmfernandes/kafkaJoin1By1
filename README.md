# Kafka Avro Producer and Join 1 by 1

Create topicA:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic topicA --partitions 1 --replication-factor 1
```

Create topicB:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic topicB --partitions 1 --replication-factor 1
```

Register schemas:

```bash
jq '. | {schema: tojson}' src/main/resources/avro/inputTopic.avsc | \
curl -X POST http://localhost:8081/subjects/topicA-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-


jq '. | {schema: tojson}' src/main/resources/avro/inputTopic.avsc | \
curl -X POST http://localhost:8081/subjects/topicB-value/versions \
-H "Content-Type: application/vnd.schemaregistry.v1+json" \
-d @-
```

Run io.confluent.csta.kafka1by1.avro.AvroProducer this once to start populate one by one messages into each topic.
Specify first topicA.

Run again in parallel io.confluent.csta.kafka1by1.avro.AvroProducer but specify now topicB.

Create on KSQLDB:

```sql

CREATE STREAM streamA (id VARCHAR)
    WITH (kafka_topic='topicA', partitions=1, value_format='avro');

CREATE STREAM streamB (id VARCHAR)
    WITH (kafka_topic='topicB', partitions=1, value_format='avro');

CREATE STREAM streamed AS
     SELECT 
        a.id as aid,
        b.id as bid
     FROM streamA a
        LEFT JOIN streamB b WITHIN 30 SECONDS ON a.id = b.id
     EMIT CHANGES;
     
CREATE STREAM streamed2 AS
     SELECT 
        a.id as aid,
        b.id as bid
     FROM streamA a
        LEFT JOIN streamB b WITHIN 30 SECONDS GRACE PERIOD 5 SECONDS ON a.id = b.id
     EMIT CHANGES;
```

Read messages from both output topics:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic STREAMED --from-beginning --property print.timestamp=true --property print.key=true --property print.value=true

kafka-console-consumer --bootstrap-server localhost:9092 --topic STREAMED2 --from-beginning --property print.timestamp=true --property print.key=true --property print.value=true
```

If you monitor both streams STREAMED and STREAMED2 you will see the following:

- If you insert d1 on topicA  you see immediately the d1 left only message on STREAMED
- Insert now d2 on topicA you see immediately the d2 left only message on STREAMED and the d1 on STREAMED2
- Insert now d3 on topicA and also d3 on topicB, you see on STREAMED both the left only d3 and the joined d3 while on STREAMED2 you see the left only d2 and the joined d3

The way it looks: 
- If you specify a grace period the left only will never be emited until some other event is consumed from left topic. And in case there is a join only the join event is emited.
- If you don't specify a grace period (deprecated) the left only is immediately emited independently of future join events being also emited when join is detected.
