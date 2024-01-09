package io.confluent.csta.kafka1by1.avro;

import io.confluent.csta.kafka1by1.InputTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class DummyKeys {
    private static final Logger log = LoggerFactory.getLogger(DummyKeys.class);
    private static final String DUMMY_ID="DUMMY";
    private static final int NUMBER_OF_TESTS=20;

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(AvroProducer.class.getResourceAsStream("/configuration.properties"));

        Scanner scanner = new Scanner(System.in);
        System.out.println("Please tell topic name for dummy messages:");
        String topic = scanner.nextLine();

        Producer<String, InputTopic> producer = new KafkaProducer<>(properties);

        try {
            //send with callback
            for(int i=0;i<NUMBER_OF_TESTS;i++) {
                InputTopic inputTopicMessage = InputTopic.newBuilder()
                        .setId(DUMMY_ID+i)
                        .build();
                ProducerRecord<String, InputTopic> record = new ProducerRecord<>(topic,
                        DUMMY_ID+i, inputTopicMessage);
                producer.send(record, (metadata, e) -> {
                    if (e != null)
                        log.info("Send failed for test dummy record {}", record, e);
                    else
                        log.info("Sent test dummy event key={}, id={} - Partition-{} - Offset {}", record.key(),
                                record.value().getId(), metadata.partition(), metadata.offset());
                });
            }
        } finally {
            log.info("Closing dummy test producer");
            producer.close();
        }

    }
}
