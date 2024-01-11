# Kafka Join 1 by 1

Start CP including SR, Control Center and KSQLDB:

```bash
docker compose up -d
```

Create topicA:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic topicA --partitions 2 --replication-factor 1
```

Create topicB:

```bash
kafka-topics --bootstrap-server localhost:9092 --create --topic topicB --partitions 2 --replication-factor 1
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

```
CREATE STREAM streamA (original_id VARCHAR KEY)
    WITH (kafka_topic='topicA', partitions=2, value_format='avro');
```

```
CREATE STREAM streamB (original_id VARCHAR KEY)
    WITH (kafka_topic='topicB', partitions=2, value_format='avro');
```

```
CREATE STREAM streamed AS
     SELECT 
        a.original_id as aid,
        b.original_id as bid
     FROM streamA a
        LEFT JOIN streamB b WITHIN 15 SECONDS ON a.original_id = b.original_id
     EMIT CHANGES;
```

```     
CREATE STREAM streamed2 AS
     SELECT 
        a.original_id as aid,
        b.original_id as bid
     FROM streamA a
        LEFT JOIN streamB b WITHIN 15 SECONDS GRACE PERIOD 20 SECONDS ON a.original_id = b.original_id
     EMIT CHANGES;
```

Read messages from both output topics:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic STREAMED --from-beginning --property print.timestamp=true --property print.key=true --property print.value=true
```

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic STREAMED2 --from-beginning --property print.timestamp=true --property print.key=true --property print.value=true
```

If you monitor both streams STREAMED and STREAMED2 you will see the following:

- If you insert d1 on topicA you see immediately the d1 left only message on STREAMED
- After 35s has passed insert now d2 on topicA you see immediately the d2 left only
  message on STREAMED and nothing yet on STREAMED2
- Try now with d3 on topicA after 35s as before, and you should see now d2 left only also on STREAMED2 (it
  hit same partition as d3)
- Insert d4 on topicA and at right after d4 on topicB, you see on
  STREAMED both the left only d4 and the joined d4 while on STREAMED2 you see the left only d1 (it hit same partition as
  d4) and the joined d4

The way it looks:

- If you specify a grace period the left only will never be emitted until some other event is consumed from left topic.
  And in case there is a join only the join event is emitted.
- If you don't specify a grace period (deprecated) the left only is immediately emitted independently of future join
  events being also emitted when join is detected.

Basically the join streaming mode has changed from eager to lazy now with grace period being required.
And at same time the join is controlled by the streaming time which requires new events to be consumed, without new
events the streaming time doesn't move forward and the last left only event standing is not emitted.

A possible way to avoid this would be a dummy producer that guarantees to send events to all partitions.

So let's run in parallel to our io.confluent.csta.kafka1by1.avro.AvroProducer execution instances an instance of
io.confluent.csta.kafka1by1.avro.SimpleDummyProducer using as topic topicA.

You will see some left only dummy on the consumer of the streamed topics you can ignore. But anyway you can start seeing
that with some delay those left only events are also being emitted on STREAMED2 now.

- Insert d5 on topicA (hits partition 0). You will see the left only d5 being emitted on STREAMED and if you wait 35s 
to 42s the left only event for d5 should also be emitted on STREAMED2.
- Insert d6 on topicA (hits partition 1). Again after 35s-42s you should see also the left only join event on 
  STREAMED2.

