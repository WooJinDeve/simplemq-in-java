package org.simplemq.queue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.simplemq.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingSimpleMqQueue implements SimpleMqQueue{

    private final Logger log = LoggerFactory.getLogger(BlockingSimpleMqQueue.class);
    private final int capacity;
    private final Map<Topic, BlockingQueue<Message<?>>> blockingQueue;

    public BlockingSimpleMqQueue(int capacity) {
        this.capacity = capacity;
        this.blockingQueue = new ConcurrentHashMap<>();
    }

    @Override
    public <T> void put(Topic topic, Message<T> message) {
        try {
            if(size(topic) == capacity){
                log.info("Topic : {}, Data was lost because the queue capacity was exceeded.", topic.topicName());
                return;
            }

            BlockingQueue<Message<?>> queue = blockingQueue.get(topic);
            queue.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }

    @Override
    public Message<?> poll(Topic topic) {
        try {
            if (!this.blockingQueue.containsKey(topic)) return null;
            BlockingQueue<?> queue = this.blockingQueue.get(topic);
            return (Message<?>) queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public int size(Topic topic) {
        if(!checkExistTopic(topic)) return 0;
        return blockingQueue.get(topic).size();
    }

    private boolean checkExistTopic(final Topic topic) {
        if (!blockingQueue.containsKey(topic)) {
            subscribe(topic);
            log.info("subscribe topic : [{}]", topic.topicName());
            return false;
        }
        return true;
    }

    private void subscribe(final Topic topic){
        final BlockingQueue<Message<?>> newTopicQueue = new LinkedBlockingQueue<>(capacity);
        blockingQueue.put(topic, newTopicQueue);
    }
}
