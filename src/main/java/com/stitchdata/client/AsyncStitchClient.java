package com.stitchdata.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.ArrayList;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.cognitect.transit.Writer;
import com.cognitect.transit.Reader;
import com.cognitect.transit.TransitFactory;


public class AsyncStitchClient implements Closeable {

    private static final int CAPACITY = 10000;
    private final BlockingQueue<QueueItem> queue = new ArrayBlockingQueue<QueueItem>(CAPACITY);
    private final int maxBytes;
    private final int maxFlushIntervalMillis;
    private long lastFlushTime;
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private StitchClient client;

    public AsyncStitchClient(String stitchUrl, int maxBytes, int maxFlushIntervalMillis) {
        this.client = new StitchClient(stitchUrl);
        this.maxBytes = maxBytes;
        this.maxFlushIntervalMillis = maxFlushIntervalMillis;
        Thread workerThread = new Thread(new Worker());
        workerThread.start();
    }

    private class QueueItem {
        byte[] bytes;
        boolean isEndOfStream;
        QueueItem(byte[] bytes, boolean isEndOfStream) {
            this.bytes = bytes;
            this.isEndOfStream = isEndOfStream;
        }
    }

    private QueueItem toQueueItem(StitchMessage message) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.MSGPACK, baos);
        writer.write(message.toMap());
        return new QueueItem(baos.toByteArray(), false);
    }

    public void close() {
        try {
            queue.put(new QueueItem(null, true));
            closeLatch.await();
        } catch (InterruptedException e) {
            return;
        }
    }

    private class Worker implements Runnable {
        private ArrayList<QueueItem> items;
        private int numBytes;

        public boolean offer(StitchMessage m) {
            return queue.offer(toQueueItem(m));
        }

        public boolean offer(StitchMessage m, long timeout, TimeUnit unit) throws InterruptedException {
            return queue.offer(toQueueItem(m), timeout, unit);
        }

        public void put(StitchMessage m) throws InterruptedException {
            queue.put(toQueueItem(m));
        }

        public boolean shouldFlush() {
            return
                numBytes >= maxBytes ||
                (System.currentTimeMillis() - lastFlushTime ) >= maxFlushIntervalMillis;

        }

        private void flush() {
            ArrayList messages = new ArrayList(items.size());
            for (QueueItem item : items) {
                ByteArrayInputStream bais = new ByteArrayInputStream(item.bytes);
                Reader reader = TransitFactory.reader(TransitFactory.Format.MSGPACK, bais);
                messages.add(reader.read());
            }

            client.pushMaps(messages);

            items.clear();
            numBytes = 0;
            lastFlushTime = System.currentTimeMillis();
        }

        public void run() {
            boolean running = true;
            while (running) {
                QueueItem item;
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
