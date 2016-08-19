package com.stitchdata.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import com.cognitect.transit.Writer;
import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;

public class Buffer {

    static final int MAX_BATCH_SIZE_BYTES = 4000000;
    static final int MAX_MESSAGES_PER_BATCH = 10000;

    private final Queue<Entry> queue = new LinkedList<Entry>();
    private int availableBytes = 0;

    // Synchronized methods for accessing the properties (queue and
    // availableBytes)

    private synchronized void putEntry(Entry entry) {
        queue.add(entry);
        availableBytes += entry.bytes.length;
    }

    private synchronized List<Entry> takeEntries(int batchSizeBytes, int batchDelayMillis) {
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

    // Static stuff. Class for wrapping a Map in an Entry, and method
    // for serializing a list of entries. Since these don't access
    // queue or availableBytes, they don't need to be synchronized.

    private static class Entry {
        byte[] bytes;
        private long entryTime;

        private Entry(Map map) {
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

    private static String serializeEntries(List<Entry> entries) throws UnsupportedEncodingException {
        if (entries == null) {
            return null;
        }

        ArrayList<Map> messages = new ArrayList<Map>();

        for (Entry entry : entries) {
            ByteArrayInputStream bais = new ByteArrayInputStream(entry.bytes);
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            messages.add((Map)reader.read());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messages);
        return baos.toString("UTF-8");
    }

    public void putMessage(Map map) {
        putEntry(new Entry(map));
    }

    public String takeBatch(int batchSizeBytes, int batchDelayMillis) throws IOException {
        return serializeEntries(takeEntries(batchSizeBytes, batchDelayMillis));
    }

}
