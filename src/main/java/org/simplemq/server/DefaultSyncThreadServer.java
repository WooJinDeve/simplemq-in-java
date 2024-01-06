package org.simplemq.server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.simplemq.queue.BlockingSimpleMqQueue;

public class DefaultSyncThreadServer implements Server {
    private static final int DEFAULT_CAPACITY = 20;
    private final Executor executor;
    private final RequestHandler requestHandler;

    public DefaultSyncThreadServer(int capacity) {
        this.executor = Executors.newSingleThreadExecutor();
        this.requestHandler = new RequestHandler(new BlockingSimpleMqQueue(capacity));
    }

    public DefaultSyncThreadServer(){
        this(DEFAULT_CAPACITY);
    }

    @Override
    public void start(int port) {
        executor.execute(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                while (true) {
                    final Socket socket = serverSocket.accept();
                    handleClient(socket);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                close();
            }
        });
    }

    private void handleClient(Socket socket) {
        try (
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream())
        ) {
            if(!requestHandler.handleRequest(in, out)){
                close(socket, in, out);
            };
        } catch (IOException e) {
            e.printStackTrace();
            close(socket, null, null);
        }
    }


    private void close(Socket socket, ObjectInputStream in, ObjectOutputStream out) {
        try {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void close() {
        return;
    }
}
