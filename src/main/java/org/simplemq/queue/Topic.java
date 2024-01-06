package org.simplemq.queue;

import java.io.Serializable;
import java.util.Objects;

public record Topic(String topicName) implements Serializable {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Topic topic)) {
            return false;
        }
        return Objects.equals(topicName, topic.topicName);
    }

}
