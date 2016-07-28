package com.stitchdata.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Map;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.cognitect.transit.Writer;
import com.cognitect.transit.Reader;
import com.cognitect.transit.TransitFactory;


public class AsyncStitchClient implements Closeable {

    private static final int CAPACITY = 10000;

    private final BlockingQueue<MessageWrapper> queue = new ArrayBlockingQueue<MessageWrapper>(CAPACITY);

    private int maxBytes;
    private int maxFlushIntervalMillis;
    private long lastFlushTime;
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private StitchClient client;

    public AsyncStitchClient(int clientId, String token, String namespace, int maxFlushIntervalMillis, int maxBytes) {
        this(new StitchClient(clientId, token, namespace), maxBytes, maxFlushIntervalMillis);
    }

    public AsyncStitchClient(String stitchUrl, int clientId, String token, String namespace, int maxFlushIntervalMillis, int maxBytes, int maxRecords) {
        this(new StitchClient(stitchUrl, clientId, token, namespace), maxBytes, maxFlushIntervalMillis);
    }

    public boolean offer(Map m) {
        return queue.offer(wrap(m));
    }

    public boolean offer(Map m, long timeout, TimeUnit unit) throws InterruptedException {
        return queue.offer(wrap(m), timeout, unit);
    }

    public void put(Map m) throws InterruptedException {
        queue.put(wrap(m));
    }

    private AsyncStitchClient(StitchClient client, int maxBytes, int maxFlushIntervalMillis) {
        this.client = client;
        this.maxBytes = maxBytes;
        this.maxFlushIntervalMillis = maxFlushIntervalMillis;
        Thread workerThread = new Thread(new Worker());
        workerThread.start();
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
        private ArrayList<MessageWrapper> items;
        private int numBytes;

        public boolean shouldFlush() {
            return
                numBytes >= maxBytes ||
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
                client.push(messages);
            } catch (StitchException e) {

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

                items.add(item);
                numBytes += item.bytes.length;
                if (shouldFlush()) {
                    flush();
                }
            }
        }
    }

}
