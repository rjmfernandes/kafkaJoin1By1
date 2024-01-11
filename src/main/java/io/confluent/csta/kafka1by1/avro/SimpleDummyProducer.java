package io.confluent.csta.kafka1by1.avro;

import io.confluent.csta.kafka1by1.InputTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.Scanner;

public class SimpleDummyProducer {
    private static final Logger log = LoggerFactory.getLogger(SimpleDummyProducer.class);
    private static boolean running = true;
    private static final long SLEEP_TIME_MS = 7000;
    private static final String DUMMY_ID="DUMMY";
    private static final int NUMBER_OF_PARTITIONS=2;

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(AvroProducer.class.getResourceAsStream("/configuration.properties"));

        Scanner scanner = new Scanner(System.in);
        System.out.println("Please tell topic name for dummy messages:");
        String topic = scanner.nextLine();

        Producer<String, InputTopic> producer = new KafkaProducer<>(properties);

        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's set running to false and interrupt thread...");
            running = false;
            mainThread.interrupt();
        }));

        try {
            while (running) {
                //send with callback
                for(int partition=0;partition<NUMBER_OF_PARTITIONS;partition++) {
                    InputTopic inputTopicMessage = InputTopic.newBuilder()
                            .setId(DUMMY_ID+partition)
                            .setJoiningDateTime(Instant.now())
                            .build();
                    ProducerRecord<String, InputTopic> record = new ProducerRecord<>(topic,partition,
                            DUMMY_ID+partition, inputTopicMessage);
                    producer.send(record, (metadata, e) -> {
                        if (e != null)
                            log.info("Send failed for dummy record {}", record, e);
                        else
                            log.info("Sent dummy event key={}, id={}, time={} - Partition-{} - Offset {}", record.key(),
                                    record.value().getId(), record.value().getJoiningDateTime().getEpochSecond(),
                                    metadata.partition(), metadata.offset());
                    });
                }
                //sleep
                Thread.sleep(SLEEP_TIME_MS);
            }
        } catch (InterruptedException e) {
            log.info("Thread interrupted.");
        } finally {
            log.info("Closing dummy producer");
            producer.close();
        }

    }
}
