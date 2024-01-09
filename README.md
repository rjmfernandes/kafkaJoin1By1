# Kafka Avro Producer and Join 1 by 1

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

```sql
CREATE STREAM streamA (id VARCHAR)
    WITH (kafka_topic='topicA', partitions=2, value_format='avro');
```

```sql
CREATE STREAM streamB (id VARCHAR)
    WITH (kafka_topic='topicB', partitions=2, value_format='avro');
```

```sql
CREATE STREAM streamed AS
     SELECT 
        a.id as aid,
        b.id as bid
     FROM streamA a
        LEFT JOIN streamB b WITHIN 30 SECONDS ON a.id = b.id
     EMIT CHANGES;
```

```sql     
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
```

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic STREAMED2 --from-beginning --property print.timestamp=true --property print.key=true --property print.value=true
```

If you monitor both streams STREAMED and STREAMED2 you will see the following:

- If you insert d1 on topicA you see immediately the d1 left only message on STREAMED
- After the window period and grace period has passed insert now d2 on topicA you see immediately the d2 left only
  message on STREAMED and nothing yet on STREAMED2
- Try now with d3 on topicA after waiting same time as before, and you should see now d2 left only also on STREAMED2 (it
  hit same partition as d3)
- Insert After the window period and grace period has passed d4 on topicA and at right after d4 on topicB, you see on
  STREAMED both the left only d4 and the joined d4 while on STREAMED2 you see the left only d1 (it hit same partition as
  d4) and the joined d4

The way it looks:

- If you specify a grace period the left only will never be emitted until some other event is consumed from left topic.
  And in case there is a join only the join event is emitted.
- If you don't specify a grace period (deprecated) the left only is immediately emitted independently of future join
  events being also emitted when join is detected.

Basically the join streaming mode has changed from eager to lazy now with grace period being required.
And at same time the time is controlled by the streaming time which requires new events to be consumed, without new
events the streaming time doesn't move forward and the last left only event standing is not emitted.

A possible way to avoid this would be a dummy producer that guarantees to send events to all partitions.

So let's run in parallel to our io.confluent.csta.kafka1by1.avro.AvroProducer execution instances an instance of
io.confluent.csta.kafka1by1.avro.DummyProducer using as topic topicA.

You will see some left only dummy on the consumer of the streamed topics you can ignore. But anyway you can start seeing
that with some delay those left only events are also being emitted on STREAMED2 now.

- Insert d5 on topicA. You will see the left only d5 being emitted on STREAMED and if you wait more than the time window
and grace period the left only event for d5 should also be emitted on STREAMED2.

If you check the code for io.confluent.csta.kafka1by1.avro.DummyProducer you can see that it explicitly has hardcoded
certain keys to use for hitting each partition:

```java
private static final String[] DUMMY_KEYS= new String[] {"DUMMY2","DUMMY0"};
```

Those strings can be found by executing io.confluent.csta.kafka1by1.avro.DummyKeys that will send records with 
DUMMY&lt;i&gt; with i from 0 to 20 so that on logs you can see to which partition each key is sent and pick one hitting 
each partition. In this case we chose the two listed before. If you try in place of this to send any chosen key for a 
partition explicitly as for example DUMMY0 and DUMMY1 (that naturally would both hit partition 1) it won't necessarily
work since during the ksqldb join processing it will potentially only work for one of the partitions (the one for which 
they would naturally correspond per partitioning being used).

If you want to see this in action stop the DummyProducer and start io.confluent.csta.kafka1by1.avro.WrongDummyProducer
(it uses DUMMY0 and DUMMY1 explicitly pointing tyo each partition even if naturally they would correspond both to 
partition 1) using as topic topicA then:

- Insert into topicA e5 (it will hit partition 1) and immediately you will the left only event on STREAMED and if you 
wait 35s you will also see on STREAMED2. So far so good.
- Insert now into topicA e6 (it will hit partition 0) and again you see on STREAMED but this time it will never show up 
on STREAMED2 (at least until you send an event to partition 0 which key would correspond to partition 0 naturally).

