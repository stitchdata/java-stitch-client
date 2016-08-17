package com.stitchdata.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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

    private static class Entry {
        byte[] bytes;
        private long entryTime;

        private Entry(Map map) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
            writer.write(map);

            bytes = baos.toByteArray();
            entryTime = System.currentTimeMillis();

            if (bytes.length > MAX_BATCH_SIZE_BYTES - 2) {
                throw new IllegalArgumentException(
                    "Can't accept a record larger than " + (MAX_BATCH_SIZE_BYTES - 2)
                    + " bytes");
            }
        }
    }

    public void putMessage(Map map) {
        Entry entry = new Entry(map);
        queue.add(entry);
        availableBytes += entry.bytes.length;
    }

    public String takeBatch(int batchSizeBytes, int batchDelayMillis) throws IOException {

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

        ArrayList<Map> messages = new ArrayList<Map>();

        // Start size at 2 to allow for opening and closing brackets
        int size = 2;
        while (!queue.isEmpty() &&
               size + queue.peek().bytes.length < MAX_BATCH_SIZE_BYTES) {
            Entry entry = queue.remove();
            // Add size of record plus the comma delimiter
            size += entry.bytes.length + 1;
            availableBytes -= entry.bytes.length;
            Map map;
            ByteArrayInputStream bais = new ByteArrayInputStream(entry.bytes);
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            messages.add((Map)reader.read());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messages);
        return baos.toString("UTF-8");
    }

}
