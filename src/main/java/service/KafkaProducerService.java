package service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.*;
import com.mongodb.client.model.DBCollectionFindOptions;
import models.StudentInfo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerService {

    private static KafkaProducerService kafkaProducerService;
    private static ObjectMapper objectMapper;

    private KafkaProducerService() {
    }

    public static KafkaProducerService getKafkaProducerServiceInstance() {

        if (kafkaProducerService == null) {
            kafkaProducerService = new KafkaProducerService();
            objectMapper = new ObjectMapper();
        }

        return kafkaProducerService;
    }

    public void produceDataToTopic() throws IOException {

        ServerAddress serverAddress[] = new ServerAddress[3];

        serverAddress[0] = new ServerAddress("localhost", 27017);
        serverAddress[1] = new ServerAddress("localhost", 27018);
        serverAddress[2] = new ServerAddress("localhost", 27019);

        // Creating Mongo Client
        MongoClient mongoClient = new MongoClient(Arrays.asList(serverAddress));

        // Getting MongoDB
        DB database = mongoClient.getDB("local");

        // Getting the collection
        DBCollection mongoCollection = database.getCollection("oplog.rs");

        DBObject query = BasicDBObjectBuilder.start().add("ns", "college.studentInfo").get();
        DBCollectionFindOptions dbCollectionFindOptions = new DBCollectionFindOptions();
        dbCollectionFindOptions.cursorType(CursorType.TailableAwait);
        dbCollectionFindOptions.maxAwaitTime(1, TimeUnit.DAYS);

        DBCursor cursor = mongoCollection.find(query, dbCollectionFindOptions);

        while (true) {
            while (cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                StudentInfo studentInfo = objectMapper.readValue(dbObject.get("o").toString(), StudentInfo.class);

                // Kafka producers logic

                if (studentInfo.getName() != null && studentInfo.getRegNo() != null) {
                    KafkaProducer<String, String> kafkaproducer = getProducer();

                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("studentInfo", studentInfo.getRegNo(), studentInfo.getName());

                    kafkaproducer.send(producerRecord, (recordMetaData, exception) -> {
                        if (recordMetaData != null) {
                            System.out.println("Record sent to topic successfully.");
                        } else {
                            System.out.println("Error in sending the data to the topic.");
                        }
                    });
                }

            }
        }
    }

    private KafkaProducer<String, String> getProducer() {

        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer(properties);
    }
}
