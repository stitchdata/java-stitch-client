package com.stitchdata.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;
import org.junit.*;
import static org.junit.Assert.*;

public class StitchClientTest  {

    private static final int NUM_THREADS = 4;
    private static final int NUM_RECORDS_PER_THREAD = 10000;

    List<AtomicInteger> numRecordsByThreadId = new ArrayList<AtomicInteger>();

    private class DummyStitchClient extends StitchClient {

        DummyStitchClient() {
            super(PUSH_URL, 0, null, null, null, Arrays.asList(new String[] { "id" }), StitchClientBuilder.DEFAULT_BATCH_SIZE_BYTES, 60000000);
        }

        @Override
        void sendBatch(String batch) throws IOException {

            ByteArrayInputStream bais = new ByteArrayInputStream(batch.getBytes());
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            List records = reader.read();
            int counts[] = new int[NUM_THREADS];
            for (int i = 0; i < NUM_THREADS; i++) {
                counts[i] = 0;
            }
            for (Object record : records) {
                Map data = (Map) ((Map)record).get("data");
                int threadId = ((Long) ((Map)data).get("threadId")).intValue();
                counts[threadId]++;
                numRecordsByThreadId.get(threadId).incrementAndGet();
            }

            // For debugging
            // System.err.print("Sent a batch of size " + batch.length() + "; counts are");
            // for (int i = 0; i < NUM_THREADS; i++) {
            //     System.err.print(" " + counts[i]);
            // }
            // System.err.println("");
        }
    }

    @Before
    public void clearNumRecordsByThreadId() {
        for (int i = 0; i < NUM_THREADS; i++) {
            numRecordsByThreadId.add(new AtomicInteger(0));
        }
    }

    public static class Sender implements Runnable {
        private Map record = new HashMap();
        private final int threadId;
        private final StitchClient stitch;

        public Sender(StitchClient stitch, int threadId) {
            this.threadId = threadId;
            this.stitch = stitch;

            char chars[] = new char[100];
            Arrays.fill(chars, 'b');
            record.put("threadId", threadId);
            record.put("a", new String(chars));
        }

        public void run() {
            for (int recordId = 0; recordId < NUM_RECORDS_PER_THREAD; recordId++) {
                record.put("recordId", recordId);
                try {
                    stitch.push(StitchMessage.newUpsert()
                                .withSequence(0)
                                .withData(record));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Test
    public void testConcurrentPushes() throws IOException {

        List<Thread> threads = new ArrayList<Thread>();
        final ArrayList<Map> records = new ArrayList<Map>();

        try (StitchClient stitch = new DummyStitchClient()) {
            for (int i = 0; i < NUM_THREADS; i++) {
                threads.add(new Thread(new Sender(stitch, i)));
            }

            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // Do nothing
                }
            }

            for (int i = 0; i < NUM_THREADS; i++) {
                assertEquals(NUM_RECORDS_PER_THREAD, numRecordsByThreadId.get(i).get());
            }
        }
    }
}
