package com.stitchdata.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import com.cognitect.transit.Writer;
import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;

public class Buffer {

    static final int MAX_BATCH_SIZE_BYTES = 4000000;
    static final int MAX_MESSAGES_PER_BATCH = 10000;

    private final Queue<Entry> queue = new LinkedList<Entry>();
    private int availableBytes = 0;

    synchronized void put(Entry entry) {
        queue.add(entry);
        availableBytes += entry.bytes.length;
    }

    synchronized List<Entry> take(int batchSizeBytes, int batchDelayMillis) {
        if (queue.isEmpty()) {
            return null;
        }

        boolean ready =
            availableBytes >= batchSizeBytes ||
            queue.size() >= MAX_MESSAGES_PER_BATCH ||
            System.currentTimeMillis() - queue.peek().entryTime >= batchDelayMillis;

        if (!ready) {
            return null;
        }

        ArrayList<Entry> entries = new ArrayList<Entry>();

        // Start size at 2 to allow for opening and closing brackets
        int size = 2;
        while (!queue.isEmpty() &&
               size + queue.peek().bytes.length < MAX_BATCH_SIZE_BYTES) {
            Entry entry = queue.remove();
            // Add size of record plus the comma delimiter
            size += entry.bytes.length + 1;
            availableBytes -= entry.bytes.length;
            entries.add(entry);
        }

        return entries;
    }

    static class Entry {
        byte[] bytes;
        Object callbackArg;
        private long entryTime;

        Entry(byte[] bytes, Object callbackArg) {

            this.bytes = bytes;
            this.entryTime = System.currentTimeMillis();
            this.callbackArg = callbackArg;

            // We need two extra bytes for the [ and ] wrapping the record.
            if (bytes.length > MAX_BATCH_SIZE_BYTES - 2) {
                throw new IllegalArgumentException(
                    "Can't accept a record larger than " + (MAX_BATCH_SIZE_BYTES - 2)
                    + " bytes (is " + bytes.length + "bytes)");
            }
        }
    }

}
