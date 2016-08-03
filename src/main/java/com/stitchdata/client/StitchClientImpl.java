package com.stitchdata.client;

import com.stitchdata.client.*;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.cognitect.transit.Writer;
import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.entity.ContentType;
import org.apache.http.StatusLine;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;

import javax.json.Json;
import javax.json.JsonReader;

public class StitchClientImpl implements StitchClient {

    private static final int CAPACITY = 10000;

    private static final ContentType CONTENT_TYPE = ContentType.create("application/transit+json");

    public static final int HTTP_CONNECT_TIMEOUT = 1000 * 60 * 2;
    private final int connectTimeout = HTTP_CONNECT_TIMEOUT;

    private final String stitchUrl;
    private final int clientId;
    private final String token;
    private final String namespace;
    private final String tableName;
    private final List<String> keyNames;
    private final int maxFlushIntervalMillis;
    private final int maxBytes;
    private final int maxRecords;
    private final ResponseHandler responseHandler;
    private final BlockingQueue<MessageWrapper> queue = new ArrayBlockingQueue<MessageWrapper>(CAPACITY);
    private ArrayList<MessageWrapper> items = new ArrayList<MessageWrapper>();
    private int numBytes = 0;
    private final CountDownLatch closeLatch;

    private long lastFlushTime = System.currentTimeMillis();

    private class MessageWrapper {
        byte[] bytes;
        boolean isEndOfStream;
        ResponseHandler responseHandler;
        MessageWrapper(byte[] bytes, boolean isEndOfStream, ResponseHandler responseHandler) {
            this.bytes = bytes;
            this.isEndOfStream = isEndOfStream;
            this.responseHandler = responseHandler;
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
            ArrayList<Map> messages = new ArrayList<Map>(items.size());
            for (MessageWrapper item : items) {
                ByteArrayInputStream bais = new ByteArrayInputStream(item.bytes);
                Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
                messages.add((Map)reader.read());
            }

            try {
                StitchResponse response = pushImpl(messages);
                for (int i = 0; i < items.size(); i++) {
                    ResponseHandler handler = items.get(i).responseHandler;
                    if (handler != null) {
                        handler.handleOk(messages.get(i), response);
                    }
                }
            } catch (Exception e) {
                for (int i = 0; i < items.size(); i++) {
                    ResponseHandler handler = items.get(i).responseHandler;
                    if (handler != null) {
                        handler.handleError(messages.get(i), e);
                    }
                }
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

    private void putWithDefault(Map map, String key, Object value, Object defaultValue) {
        map.put(key, value != null ? value : defaultValue);
    }

    private void putIfNotNull(Map map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private Map messageToMap(StitchMessage message) {
        HashMap map = new HashMap();

        map.put(Field.CLIENT_ID, clientId);
        map.put(Field.NAMESPACE, namespace);

        putWithDefault(map, Field.TABLE_NAME, message.getTableName(), tableName);
        putWithDefault(map, Field.KEY_NAMES, message.getKeyNames(), keyNames);

        putIfNotNull(map, Field.TABLE_VERSION, message.getTableVersion());
        putIfNotNull(map, Field.SEQUENCE, message.getSequence());
        putIfNotNull(map, Field.DATA, message.getData());

        return map;
    }

    private MessageWrapper wrap(StitchMessage message, ResponseHandler responseHandler) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messageToMap(message));
        return new MessageWrapper(baos.toByteArray(), false, responseHandler);
    }

    StitchClientImpl(String stitchUrl, int clientId, String token, String namespace, String tableName, List<String> keyNames,
                 int maxFlushIntervalMillis, int maxBytes, int maxRecords, ResponseHandler responseHandler) {
        this.stitchUrl = stitchUrl;
        this.clientId = clientId;
        this.token = token;
        this.namespace = namespace;
        this.tableName = tableName;
        this.keyNames = keyNames;
        this.maxFlushIntervalMillis = maxFlushIntervalMillis;
        this.maxBytes = maxBytes;
        this.maxRecords = maxRecords;
        this.closeLatch = new CountDownLatch(1);
        this.responseHandler = responseHandler;
        Thread workerThread = new Thread(new Worker());
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public StitchResponse push(List<StitchMessage> messages) throws StitchException, IOException {
        List<Map> maps = new ArrayList<Map>();
        for (StitchMessage message : messages) {
            maps.add(messageToMap(message));
        }
        return pushImpl(maps);
    }

    public StitchResponse pushImpl(List<Map> messages) throws StitchException, IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messages);
        String body = baos.toString("UTF-8");

        try {
            Request request = Request.Post(stitchUrl)
                .connectTimeout(connectTimeout)
                .addHeader("Authorization", "Bearer " + token)
                .bodyString(body, CONTENT_TYPE);

            HttpResponse response = request.execute().returnResponse();

            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            JsonReader rdr = Json.createReader(entity.getContent());
            StitchResponse stitchResponse = new StitchResponse(
                statusLine.getStatusCode(),
                statusLine.getReasonPhrase(),
                rdr.readObject());
            if (stitchResponse.isOk()) {
                return stitchResponse;
            }
            else {
                throw new StitchException(stitchResponse);
            }
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        }
    }

    public StitchResponse push(StitchMessage message) throws StitchException, IOException {
        ArrayList<StitchMessage> messages = new ArrayList<StitchMessage>();
        messages.add(message);
        return push(messages);
    }

    public boolean offer(StitchMessage m, ResponseHandler responseHandler) {
        return queue.offer(wrap(m, responseHandler));
    }

    public boolean offer(StitchMessage m, ResponseHandler responseHandler, long timeout) throws InterruptedException {
        return queue.offer(wrap(m, responseHandler), timeout, TimeUnit.MILLISECONDS);
    }

    public void put(StitchMessage m, ResponseHandler responseHandler) throws InterruptedException {
        queue.put(wrap(m, responseHandler));
    }

    public void close() {
        try {
            queue.put(new MessageWrapper(null, true, null));
            closeLatch.await();
        } catch (InterruptedException e) {
            return;
        }
    }

}
