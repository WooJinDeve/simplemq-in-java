package org.simplemq.server;

import static org.simplemq.server.RequestType.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.simplemq.message.Message;
import org.simplemq.queue.SimpleMqQueue;
import org.simplemq.queue.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandler {
    private final Logger log = LoggerFactory.getLogger(RequestHandler.class);
    private final SimpleMqQueue queue;
    public RequestHandler(SimpleMqQueue queue) {
        this.queue = queue;
    }

    public boolean handleRequest(ObjectInputStream in, ObjectOutputStream out) throws IOException {
        try {
            RequestType requestType = (RequestType) in.readObject();
            if (requestType == PRODUCE) {
                return write(in);
            } else if (requestType == CONSUME) {
                return read(in, out);
            }
            else return false;
        } catch (IOException | ClassNotFoundException e) {
            return false;
        }
    }

    private boolean write(ObjectInputStream in) {
        try {
            Topic topic = (Topic) in.readObject();
            Message<?> message = (Message<?>) in.readObject();
            queue.put(topic, message);
            return true;
        } catch (IOException | ClassNotFoundException e) {
            log.error("An error occurred while saving the message.");
            return false;
        }
    }


    private boolean read(ObjectInputStream in, ObjectOutputStream out){
        try {
            Topic topic = (Topic) in.readObject();
            Message<?> message = queue.poll(topic);
            out.writeObject(message);
            out.flush();
            return true;
        } catch (IOException | ClassNotFoundException e) {
            log.error("An error occurred while reading the message.");
            return false;
        }
    }
}
