package org.simplemq;

import org.simplemq.consumer.Consumer;
import org.simplemq.consumer.ConsumerProperties;
import org.simplemq.producer.Producer;
import org.simplemq.producer.ProducerProperties;
import org.simplemq.server.DefaultSyncThreadServer;
import org.simplemq.server.Server;

public class SimpleMqApplication {
    private static final int PORT = 9093;

    public static void main(String[] args) {
        SimpleMqApplication simpleMqApplication = new SimpleMqApplication();
        simpleMqApplication.start(args);
        simpleMqApplication.test();
    }

    public void start(String[] args){
        Server server = new DefaultSyncThreadServer();
        server.start(PORT);
    }

    public void test(){
        String topic = "test";
        ProducerProperties properties1 = new ProducerProperties("localhost", PORT);
        ConsumerProperties properties2 = new ConsumerProperties("localhost", PORT);
        Producer<String> producer = new Producer<>(properties1);
        Consumer<String> consumer = new Consumer<>(properties2);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(() -> {
            int i = 0;
            while (true){
                String message = "Message " + i++;
                producer.send(topic, message);
                System.out.println("Produced: " + message);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }).start();


        new Thread(() -> {
            while (true){
                String consume = consumer.consume(topic);
                System.out.println("Consumed: " + consume);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}