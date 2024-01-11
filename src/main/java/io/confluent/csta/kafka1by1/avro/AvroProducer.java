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

public class AvroProducer {
    private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);
    private static boolean running = true;
    private static final long SLEEP_TIME_MS = 500;

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(AvroProducer.class.getResourceAsStream("/configuration.properties"));

        Scanner scanner = new Scanner(System.in);
        System.out.println("Please tell topic name:");
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
                System.out.println("Please input an id:");
                String id = scanner.nextLine();
                InputTopic inputTopicMessage = InputTopic.newBuilder()
                        .setId(id).setJoiningDateTime(Instant.now())
                        .build();

                //kafka producer - asynchronous writes
                ProducerRecord<String, InputTopic> record = new ProducerRecord<>(topic, id, inputTopicMessage);
                //send with callback
                producer.send(record, (metadata, e) -> {
                    if (e != null)
                        log.info("Send failed for record {}", record, e);
                    else
                        log.info("Sent key={}, id={}, time={} - Partition-{} - Offset {}", record.key(),
                                record.value().getId(), record.value().getJoiningDateTime().getEpochSecond(),
                                metadata.partition(), metadata.offset());
                });
                //sleep
                Thread.sleep(SLEEP_TIME_MS);
            }
        } catch (InterruptedException e) {
            log.info("Thread interrupted.");
        } finally {
            log.info("Closing producer");
            producer.close();
        }

    }
}
