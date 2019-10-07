package service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerService {

    private static KafkaConsumerService kafkaConsumerService;

    private KafkaConsumerService() {
    }

    public static KafkaConsumerService getKafkaConsumerServiceInstance() {
        if (kafkaConsumerService == null) {
            kafkaConsumerService = new KafkaConsumerService();
        }

        return kafkaConsumerService;
    }

    public void consumeData() throws IOException {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "1");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("studentInfo"));
        URL fileURL = getClass().getResource("/RegNowithStudentName.txt");
        File file = new File(fileURL.getPath());
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);
            consumerRecords.forEach(consumerRecord -> {
                try {
                    System.out.println("Key : " + consumerRecord.key() + " Value : " + consumerRecord.value());
                    String data = consumerRecord.key() + " : " + consumerRecord.value();
                    bufferedWriter.write(data);
                    bufferedWriter.newLine();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            kafkaConsumer.commitAsync();
        }
    }
}
