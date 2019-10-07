import service.KafkaConsumerService;
import service.KafkaProducerService;

public class Application {

    public static void main(String args[]) {

        KafkaProducerService kafkaProducerService = KafkaProducerService.getKafkaProducerServiceInstance();
        KafkaConsumerService kafkaConsumerService = KafkaConsumerService.getKafkaConsumerServiceInstance();

        Thread producerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Starting Producer Thread.");
                try {
                    kafkaProducerService.produceDataToTopic();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Thread consumerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Starting Consumer Thread.");
                try {
                    kafkaConsumerService.consumeData();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        producerThread.start();
        consumerThread.start();
    }
}
