package io.confluent.csta.kafka1by1.avro;

import io.confluent.csta.kafka1by1.InputTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

public class GracePeriodExplorer {
    private static final Logger log = LoggerFactory.getLogger(GracePeriodExplorer.class);
    private static final int TIME_WINDOW = 30;
    private static final int GRACE_PERIOD = 60;
    private static final String TOPIC_A ="topicA";
    private static final String TOPIC_B ="topicB";

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        properties.load(GracePeriodExplorer.class.getResourceAsStream("/configuration.properties"));
        Scanner scanner = new Scanner(System.in);
        Producer<String, InputTopic> producer = new KafkaProducer<>(properties);

        System.out.printf("""
                Time window assumed of %ds.
                Grace period assumed to be %ds.
                %n""", TIME_WINDOW,GRACE_PERIOD);

        joinEventsScenario("""
                Insert into topicA event X.
                Delay half the grace period.
                Insert into topicB event X with timestamp plus half the window time.
                Click enter to execute.
                 ""","X",TIME_WINDOW/2,GRACE_PERIOD/2,
                scanner, producer);

        joinEventsScenario("""
                Insert into topicA event Y.
                Delay half the grace period.
                Insert into topicB event Y with timestamp minus the window time.
                Click enter to execute.
                 ""","Y",-TIME_WINDOW,GRACE_PERIOD/2,
                scanner, producer);

        joinEventsScenario("""
                Insert into topicA event Z.
                Delay half the grace period.
                Insert into topicB event Z with timestamp minus the double of window time.
                Click enter to execute.
                 ""","Z",-2*TIME_WINDOW,GRACE_PERIOD/2,
                scanner, producer);

        joinEventsScenario("""
                Insert into topicA event W.
                Delay the double of grace period.
                Insert into topicB event W with timestamp plus half the window time.
                Click enter to execute.
                 ""","W",TIME_WINDOW/2,2*GRACE_PERIOD,
                scanner, producer);

    }

    private static void joinEventsScenario(String msg,String id,int secondsDifference,long delay,Scanner scanner,
                                           Producer<String, InputTopic> producer) {
        System.out.println(msg);
        scanner.nextLine();
        sendEventsForJoin(id, secondsDifference,delay, producer);
        System.out.println("""
                Click enter to continue.
                """);
        scanner.nextLine();
    }

    private static void sendEventsForJoin(String id,int secondDifference,long delay,
                                          Producer<String, InputTopic> producer){
        long now=(new Date()).getTime();
        sendToTopic(id, TOPIC_A,now, producer);
        try {
            System.out.printf("Will wait for %ds%n", delay);
            Thread.sleep(delay*1000);
        } catch (InterruptedException e) {
            //nothing
        }
        sendToTopic(id, TOPIC_B,now+secondDifference* 1000L, producer);
    }

    private static void sendToTopic(String id, String topic,long timestamp,
                                    Producer<String, InputTopic> producer) {
        InputTopic inputTopicMessage = InputTopic.newBuilder()
                .setId(id)
                .build();
        ProducerRecord<String, InputTopic> record = new ProducerRecord<>(topic,null,timestamp,
                id, inputTopicMessage);
        producer.send(record, (metadata, e) -> {
            if (e != null)
                log.info("Send failed on topic {} for record {}", topic,record, e);
            else
                log.info("Sent to topic={}, key={}, id={} - Partition-{} - Offset {}", topic, record.key(),
                        record.value().getId(), metadata.partition(), metadata.offset());
        });
    }
}
