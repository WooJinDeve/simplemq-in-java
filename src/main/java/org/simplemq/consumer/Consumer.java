package org.simplemq.consumer;

import static org.simplemq.server.RequestType.CONSUME;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Objects;
import org.simplemq.message.Message;
import org.simplemq.queue.Topic;

public class Consumer<T> {
    private final ConsumerProperties properties;
    public Consumer(ConsumerProperties properties) {
        this.properties = properties;
    }

    public T consume(String topic) {
        Object message = doConsume(topic);
        return processMessage(message);
    }

    private Object doConsume(String topic) {
        try (
                Socket socket = new Socket(properties.ipAddress(), properties.port());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {
            out.writeObject(CONSUME);
            out.writeObject(new Topic(topic));
            out.flush();
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    private T processMessage(Object message) {
        return Objects.isNull(message) ? null : ((Message<T>) message).content();
    }

}
