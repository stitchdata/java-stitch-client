package com.stitchdata.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.cognitect.transit.Writer;
import com.cognitect.transit.Reader;
import com.cognitect.transit.TransitFactory;

public class AsyncStitchClient implements Closeable {

    private static final int CAPACITY = 10000;

    private static final ResponseHandler DEFAULT_RESPONSE_HANDLER = new ResponseHandler() {
            public void handleOk(List<Map> messages, StitchResponse response) { }
            public void handleError(List<Map> messages, Exception e) { }
        };

    private final StitchClient client;
    private final int maxFlushIntervalMillis;
    private final int maxBytes;
    private final int maxRecords;
    private final ResponseHandler responseHandler;

    private final BlockingQueue<MessageWrapper> queue = new ArrayBlockingQueue<MessageWrapper>(CAPACITY);
    private ArrayList<MessageWrapper> items = new ArrayList<MessageWrapper>();
    private int numBytes = 0;
    private final CountDownLatch closeLatch;

    private long lastFlushTime = System.currentTimeMillis();

    public static class Builder {

        private StitchClient.Builder clientBuilder = StitchClient.builder();
        private int maxFlushIntervalMillis = 10000;
        private int maxBytes = 10000000;
        private int maxRecords = 10000;
        private ResponseHandler responseHandler = DEFAULT_RESPONSE_HANDLER;

        public Builder withClientId(int clientId) {
            this.clientBuilder.withClientId(clientId);
            return this;
        }

        public Builder withToken(String token) {
            this.clientBuilder.withToken(token);
            return this;
        }

        public Builder withNamespace(String namespace) {
            this.clientBuilder.withNamespace(namespace);
            return this;
        }

        public Builder withMaxFlushIntervalMillis(int maxFlushIntervalMillis) {
            this.maxFlushIntervalMillis = maxFlushIntervalMillis;
            return this;
        }

        public Builder withMaxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
            return this;
        }

        public Builder withMaxRecords(int maxRecords) {
            this.maxRecords = maxRecords;
            return this;
        }

        public Builder withResponseHandler(ResponseHandler responseHandler) {
            this.responseHandler = responseHandler;
            return this;
        }

        public AsyncStitchClient build() {
            return new AsyncStitchClient(
                clientBuilder.build(),
                maxFlushIntervalMillis,
                maxBytes,
                maxRecords,
                responseHandler);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    AsyncStitchClient(StitchClient client, int maxFlushIntervalMillis, int maxBytes, int maxRecords, ResponseHandler responseHandler) {
        this.client = client;
        this.maxFlushIntervalMillis = maxFlushIntervalMillis;
        this.maxBytes = maxBytes;
        this.maxRecords = maxRecords;
        this.closeLatch = new CountDownLatch(1);
        this.responseHandler = responseHandler;
        Thread workerThread = new Thread(new Worker());
        workerThread.start();
    }

    public boolean offer(Map m) {
        return offer(m);
    }

    public boolean offer(Map m, long timeout, TimeUnit unit) throws InterruptedException {
        return offer(m, timeout, unit);
    }

    public void put(Map m) throws InterruptedException {
        queue.put(wrap(m));
    }

    private class MessageWrapper {
        byte[] bytes;
        boolean isEndOfStream;
        MessageWrapper(byte[] bytes, boolean isEndOfStream) {
            this.bytes = bytes;
            this.isEndOfStream = isEndOfStream;
        }
    }

    private MessageWrapper wrap(Map message) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.MSGPACK, baos);
        writer.write(message);
        return new MessageWrapper(baos.toByteArray(), false);
    }

    public void close() {
        try {
            queue.put(new MessageWrapper(null, true));
            closeLatch.await();
        } catch (InterruptedException e) {
            return;
        }
    }

    private class Worker implements Runnable {

        public boolean shouldFlush() {
            return
                numBytes >= maxBytes ||
                items.size() >= maxRecords ||
                (System.currentTimeMillis() - lastFlushTime ) >= maxFlushIntervalMillis;
        }

        private void flush() {
            ArrayList messages = new ArrayList(items.size());
            for (MessageWrapper item : items) {
                ByteArrayInputStream bais = new ByteArrayInputStream(item.bytes);
                Reader reader = TransitFactory.reader(TransitFactory.Format.MSGPACK, bais);
                messages.add(reader.read());
            }

            try {
                StitchResponse response = client.push(messages);
                responseHandler.handleOk(messages, response);
            } catch (Exception e) {
                responseHandler.handleError(messages, e);
            }

            items.clear();
            numBytes = 0;
            lastFlushTime = System.currentTimeMillis();
        }

        public void run() {
            boolean running = true;
            while (running) {
                MessageWrapper item;
                try {
                    item = queue.take();
                } catch (InterruptedException e) {
                    return;
                }
                if (item.isEndOfStream) {
                    running = false;
                    flush();
                    closeLatch.countDown();
                }

                else {
                    items.add(item);
                    numBytes += item.bytes.length;
                    if (shouldFlush()) {
                        flush();
                    }
                }
            }
        }
    }

    public Map newUpsertMessage(String tableName, List<String> keyNames, long sequence, Map data) {
        return client.newUpsertMessage(tableName, keyNames, sequence, data);
    }

}
