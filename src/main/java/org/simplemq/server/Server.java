package org.simplemq.server;

public interface Server {

    void start(int port);
    void close();
}
