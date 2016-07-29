package com.stitchdata.client;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import com.cognitect.transit.Writer;
import com.cognitect.transit.TransitFactory;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.entity.ContentType;
import org.apache.http.StatusLine;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;

import javax.json.Json;
import javax.json.JsonReader;

/**
 * Client for Stitch.
 *
 * This is a synchronous client.
 *
 * <pre>
 * {@code
 *
 *   // Build the client. You'll need a client id, token, and
 *   // namespace, available from http://stitchdata.com.
 *   StitchClient stitch = StitchClient.builder()
 *     .withClientId(yourClientId)
 *     .withToken(yourToken)
 *     .withNamespace(yourNamespace)
 *     .build();
 *
 *   // List of records we want to insert into a table
 *   List<Map> events = ...;
 *
 *   // The name of the table
 *   String tableName = "events";
 *
 *   // List of keys in the data map that are used to identify the record
 *   List<String> keyNames = Arrays.asList(new String[] { "eventId" });
 *
 *   // If Stitch sees multiple messages with the same identifier, it
 *   // will choose the one with the largest sequence number.
 *   long sequence = System.currentTimeMillis();
 *
 *   List<Map> messages = new ArrayList<Map>();
 *   for (Map event : events) {
 *     messages.add(stitch.newUpsertMessage(tableName, keyNames, sequence, event));
 *   }
 *
 *   try {
 *     stitch.push(messages);
 *   }
 *   catch (StitchException e) {
 *     log.error(e, "Couldn't send record");
 *   }
 * }
 * </pre>
 */
public class StitchClient {

    private static final int CAPACITY = 10000;
    public static final String STITCH_URL =  "https://pipeline-gateway.rjmetrics.com/push";
    private static final ContentType CONTENT_TYPE = ContentType.create("application/transit+json");

    private final int connectTimeout = Stitch.HTTP_CONNECT_TIMEOUT;

    private final String stitchUrl;
    private final int clientId;
    private final String token;
    private final String namespace;
    private final int maxFlushIntervalMillis;
    private final int maxBytes;
    private final int maxRecords;
    private final ResponseHandler responseHandler;
    private final BlockingQueue<MessageWrapper> queue = new ArrayBlockingQueue<MessageWrapper>(CAPACITY);
    private ArrayList<MessageWrapper> items = new ArrayList<MessageWrapper>();
    private int numBytes = 0;
    private final CountDownLatch closeLatch;

    private long lastFlushTime = System.currentTimeMillis();

    /**
     * Use this to build instances of StitchClient.
     */
    public static class Builder {
        private int clientId;
        private String token;
        private String namespace;
        private int maxFlushIntervalMillis = Stitch.DEFAULT_MAX_FLUSH_INTERVAL_MILLIS;
        private int maxBytes = Stitch.DEFAULT_MAX_FLUSH_BYTES;
        private int maxRecords = Stitch.DEFAULT_MAX_FLUSH_RECORDS;
        private ResponseHandler responseHandler = null;

        public Builder withClientId(int clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder withToken(String token) {
            this.token = token;
            return this;
        }

        public Builder withNamespace(String namespace) {
            this.namespace = namespace;
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


        public StitchClient build() {
            return new StitchClient(
                STITCH_URL, clientId, token, namespace,
                maxFlushIntervalMillis,
                maxBytes,
                maxRecords,
                responseHandler);
        }
    }

    private class MessageWrapper {
        byte[] bytes;
        boolean isEndOfStream;
        MessageWrapper(byte[] bytes, boolean isEndOfStream) {
            this.bytes = bytes;
            this.isEndOfStream = isEndOfStream;
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
                Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
                messages.add(reader.read());
            }

            try {
                StitchResponse response = client.push(messages);
                if (responseHandler != null) {
                    responseHandler.handleOk(messages, response);
                }
            } catch (Exception e) {
                if (responseHandler != null) {
                    responseHandler.handleError(messages, e);
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

    private MessageWrapper wrap(Map message) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        // using bytes to avoid storing a mutable map
        writer.write(message);
        return new MessageWrapper(baos.toByteArray(), false);
    }

    /**
     * Get a new StitchClient builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    StitchClient(String stitchUrl, int clientId, String token, String namespace) {
        this.stitchUrl = stitchUrl;
        this.clientId = clientId;
        this.token = token;
        this.namespace = namespace;
        this.maxFlushIntervalMillis = maxFlushIntervalMillis;
        this.maxBytes = maxBytes;
        this.maxRecords = maxRecords;
        this.closeLatch = new CountDownLatch(1);
        this.responseHandler = responseHandler;
        Thread workerThread = new Thread(new Worker());
        workerThread.start();
    }

    /**
     * Push a list of messages, blocking until Stitch accepts them.
     *
     * @param messages List of messages to send. Use
     *                 client.newUpsertMessage() to create messages.
     */
    public StitchResponse push(List<Map> messages) throws StitchException, IOException {
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

    public StitchResponse push(Map message) throws StitchException, IOException {
        ArrayList<Map> messages = new ArrayList<Map>();
        messages.add(message);
        return push(messages);
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

    /**
     * Create a new "upser" message.
     *
     * @param tableName name of the table to upsert into
     * @param keyNames keys into data that identify the record
     * @param sequence used to enforce ordering for records
     * @param data the record to insert, as a map
     * @return A new Map, populated with all the fields that {@link #push(Map)} requires.
     */

    public Map newUpsertMessage(String tableName, List<String> keyNames, long sequence, Map data) {
        Map<String,Object> message = new HashMap<String,Object>();
        message.put(Stitch.Field.CLIENT_ID, clientId);
        message.put(Stitch.Field.NAMESPACE, namespace);
        message.put(Stitch.Field.ACTION, Stitch.Action.UPSERT);
        message.put(Stitch.Field.TABLE_NAME, tableName);
        message.put(Stitch.Field.KEY_NAMES, keyNames);
        message.put(Stitch.Field.SEQUENCE, sequence);
        message.put(Stitch.Field.DATA, data);
        return message;
    }

    public StitchResponse pushUpsert(String tableName, List<String> keyNames, long sequence, Map data) throws StitchException, IOException {
        return push(newUpsertMessage(tableName, keyNames, sequence, data));
    }

    public void validate(Map message) {
        // TODO: Validate
    }

    public void close() {
        try {
            queue.put(new MessageWrapper(null, true));
            closeLatch.await();
        } catch (InterruptedException e) {
            return;
        }
    }

}
