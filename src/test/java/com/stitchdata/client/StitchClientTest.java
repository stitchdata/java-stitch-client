package com.stitchdata.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentSkipListSet;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;
import org.junit.*;
import static org.junit.Assert.*;

/**
 * Attempts to exercise concurrent calls to {@link
 * StitchClient#push(StitchMessage)}. Starts four threads that each
 * send 10,000 records to a {@link DummyStitchClient}, which just
 * counts the number of records received from each thread. Then we
 * assert that the correct number of records were seen. This does not
 * attempt to guarantee that calls to {@link
 * StitchClient#push(StitchMessage)} are interleaved, but there are
 * some debug print statements that we have used to show that batches
 * at least contain records from multiple threads.
 */
public class StitchClientTest  {

    private static final int NUM_THREADS = 4;
    private static final int NUM_RECORDS_PER_THREAD = 10000;

    List<AtomicInteger> numRecordsByThreadId = new ArrayList<AtomicInteger>();

    /**
     * Overrides StitchClient for test purposes. Uses a blank URL and
     * null values for most of the properties. Overrides sendBatch so
     * that it expects records that have a "threadId" field, and
     * updates numRecordsByThreadId to reflect the number of records
     * seen with that threadId.
     */
    private class DummyStitchClient extends StitchClient {

        DummyStitchClient(FlushHandler flushHandler) {
            super("", 0, null, null, null, Arrays.asList(new String[] { "id" }), StitchClientBuilder.DEFAULT_BATCH_SIZE_BYTES, 60000000, flushHandler, null);
        }

        @Override
        StitchResponse sendToStitch(String body) throws IOException {

            ByteArrayInputStream bais = new ByteArrayInputStream(body.getBytes());
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

            return new StitchResponse(200, "ok", null);

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

    /**
     * Runnable that sends 10,000 records into a StitchClient.
     */
    public static class Sender implements Runnable {
        private Map record = new HashMap();
        private final int threadId;
        private final StitchClient stitch;
        private final boolean useCallback;

        public Sender(StitchClient stitch, int threadId, boolean useCallback) {
            this.threadId = threadId;
            this.stitch = stitch;
            this.useCallback = useCallback;

            char chars[] = new char[100];
            Arrays.fill(chars, 'b');
            record.put("threadId", threadId);
            record.put("a", new String(chars));
        }

        public void run() {
            for (int recordId = 0; recordId < NUM_RECORDS_PER_THREAD; recordId++) {
                record.put("recordId", recordId);
                try {
                    StitchMessage message = StitchMessage.newUpsert()
                        .withSequence(0)
                        .withData(record);
                    if (useCallback) {
                        stitch.push(message, String.format("thread-%d-record-%d", threadId, recordId));
                    }
                    else {
                        stitch.push(message);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Test
    public void testConcurrentPushesWithoutCallback() throws IOException {

        try (StitchClient stitch = new DummyStitchClient(null)) {

            // Initialize and start all the threads
            List<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < NUM_THREADS; i++) {
                threads.add(new Thread(new Sender(stitch, i, false)));
            }
            for (Thread thread : threads) {
                thread.start();
            }

            // Wait until the threads are done sending
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // Do nothing
                }
            }
        }

        // Each thread should have sent the correct number of records
        for (int i = 0; i < NUM_THREADS; i++) {
            assertEquals(NUM_RECORDS_PER_THREAD, numRecordsByThreadId.get(i).get());
        }

    }

    private static class SetFlushHandler implements FlushHandler {
        final ConcurrentSkipListSet callbackArgsReceived =
            new ConcurrentSkipListSet();
        public void onFlush(List arg) {
            callbackArgsReceived.addAll(arg);
        }
    }

    @Test
    public void testConcurrentPushesWithCallback() throws IOException {

        SetFlushHandler flushHandler = new SetFlushHandler();
        try (StitchClient stitch = new DummyStitchClient(flushHandler)) {

            // Initialize and start all the threads
            List<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < NUM_THREADS; i++) {
                threads.add(new Thread(new Sender(stitch, i, true)));
            }
            for (Thread thread : threads) {
                thread.start();
            }

            // Wait until the threads are done sending
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // Do nothing
                }
            }
        }
        // Each thread should have sent the correct number of records
        for (int i = 0; i < NUM_THREADS; i++) {
            assertEquals(NUM_RECORDS_PER_THREAD, numRecordsByThreadId.get(i).get());
        }

        assertEquals(NUM_THREADS * NUM_RECORDS_PER_THREAD, flushHandler.callbackArgsReceived.size());
    }
}
