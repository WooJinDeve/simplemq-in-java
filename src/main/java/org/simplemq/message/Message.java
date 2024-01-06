package org.simplemq.message;

import java.io.Serializable;

public record Message<T>(T content) implements Serializable {
}
