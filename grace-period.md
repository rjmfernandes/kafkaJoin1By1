You can stop now the current java classes (both AvroProducers and the SimpleDummyProducer).

Run GracePeriodExplorer to check what you see regarding grace period when using header timestamp only.

- All the scenarios except last one are easily explained considering just if the events time match within the join
  window. The Z one doesn't join because does not match that window.
- The last W one is a bit unexpected because it shouldn't join considering it elapsed longer than the grace period
  accepts. Maybe the reason lies with the fact that we are using header time?

Let's check that and create new streams in KSQLDB:

```
CREATE STREAM streamA2 (original_id VARCHAR KEY, joiningDateTime BIGINT)
    WITH (kafka_topic='topicA', partitions=2, value_format='avro', timestamp='joiningDateTime');
```

```
CREATE STREAM streamB2 (original_id VARCHAR KEY, joiningDateTime BIGINT)
    WITH (kafka_topic='topicB', partitions=2, value_format='avro', timestamp='joiningDateTime');
```

```
CREATE STREAM streamed3 AS
     SELECT 
        a.original_id as aid,
        b.original_id as bid,
        a.joiningDateTime as aJoiningDateTime,
        b.joiningDateTime as bJoiningDateTime
     FROM streamA2 a
        LEFT JOIN streamB2 b WITHIN 15 SECONDS ON a.original_id = b.original_id
     EMIT CHANGES;
```

```     
CREATE STREAM streamed4 AS
     SELECT 
        a.original_id as aid,
        b.original_id as bid,
        a.joiningDateTime as aJoiningDateTime,
        b.joiningDateTime as bJoiningDateTime
     FROM streamA2 a
        LEFT JOIN streamB2 b WITHIN 15 SECONDS GRACE PERIOD 20 SECONDS ON a.original_id = b.original_id
     EMIT CHANGES;
```

Now we can start consuming messages from both last two streams:

```bash
kafka-avro-console-consumer --topic STREAMED3 --bootstrap-server localhost:9092 --property schema.registry.url=http://127.0.0.1:8081 --from-beginning
```

```bash
kafka-avro-console-consumer --topic STREAMED4 --bootstrap-server localhost:9092 --property schema.registry.url=http://127.0.0.1:8081 --from-beginning
```

We run now GracePeriodExplorerPayloadTimestamp to check what we see regarding grace period when using payload timestamp.

We see exactly the same behaviour as before. For some reason grace period is not being enforced to discard join events.

