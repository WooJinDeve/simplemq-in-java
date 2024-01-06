package org.simplemq.producer;

import static org.simplemq.server.RequestType.PRODUCE;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import org.simplemq.message.Message;
import org.simplemq.queue.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer<T> {
    private final Logger log = LoggerFactory.getLogger(Producer.class);
    private final ProducerProperties properties;

    public Producer(ProducerProperties properties) {
        this.properties = properties;
    }

    public void send(String topic, T message) {
        boolean flag = doSend(new Topic(topic), new Message<>(message));
        if (flag) log.info("topic : [{}] send message", topic);
    }

    public boolean doSend(Topic topic, Message<T> message){
        try (
                Socket socket = new Socket(properties.ipAddress(), properties.port());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())
        ) {
            out.writeObject(PRODUCE);
            out.writeObject(topic);
            out.writeObject(message);
            out.flush();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
