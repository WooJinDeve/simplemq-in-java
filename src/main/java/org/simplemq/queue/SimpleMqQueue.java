package org.simplemq.queue;

import org.simplemq.message.Message;

public interface SimpleMqQueue {
    <T>void put(final Topic topic, final Message<T> t);
    Message<?> poll(final Topic topic);

    int size(final Topic topic);
}
