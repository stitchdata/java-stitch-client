package com.stitchdata.client;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import com.cognitect.transit.Writer;
import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;

public class Buffer {

    private final int capacity;
    private final List<ByteArrayWrapper> buffer = new ArrayList<ByteArrayWrapper>();
    private int size;

    private class ByteArrayWrapper {
        byte[] bytes;
    }

    Buffer(int capacity) {
        this.capacity = capacity;
    }

    public void write(Map map) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(map);
        ByteArrayWrapper wrapper = new ByteArrayWrapper();
        wrapper.bytes = baos.toByteArray();
        size += wrapper.bytes.length;
    }

    public String read() throws IOException {
        ArrayList<Map> messages = new ArrayList<Map>(buffer.size());
        for (ByteArrayWrapper wrapper : buffer) {
            ByteArrayInputStream bais = new ByteArrayInputStream(wrapper.bytes);
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            messages.add((Map)reader.read());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messages);
        return baos.toString("UTF-8");
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean isFull() {
        return size >= capacity;
    }

    public void clear() {
        buffer.clear();
    }

}
