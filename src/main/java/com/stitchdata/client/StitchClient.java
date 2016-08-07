package com.stitchdata.client;

import java.io.Closeable;
import java.io.EOFException;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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
 * <p>Callers should use {@link StitchClientBuilder} to construct
 * instances of {@link StitchClient}.</p>
 *
 * A StitchClient maintains a fixed-capacity buffer for
 * messages. Every call to {@link StitchClient#push(StitchMessage)}
 * adds a record to the buffer and then delivers any outstanding
 * messages if the buffer is full or if too much time has passed since
 * the last flush. You should call {@link StitchClient#close()} when
 * you are finished sending records or you will lose any records that
 * have been added to the buffer but not yet delivered. Buffer
 * parameters can be configured with {@link
 * StitchClientBuilder#withBufferCapacity(int)} and {@link
 * StitchClientBuilder#withBufferTimeLimit(int)}.
 *
 * <pre>
 * {@code
 * StitchClient stitch = new StitchClientBuilder()
 *   .withClientId(123)
 *   .withToken("asdfasdfasdfasdasdfasdfadsfadfasdfasdfadfsasdf")
 *   .withNamespace("event_tracking")
 *   .withTableName("events")
 *   .withKeyNames(thePrimaryKeyFields)
 *   .build();
 *
 * try {
 *
 *     for (Map data : someSourceOfRecords) {
 *         stitch.push(StitchMessage.newUpsert()
 *             .withSequence(System.currentTimeMillis())
 *             .withData(data));
 *     }
 * }
 * catch (StitchException e) {
 *     System.err.println("Error sending to stitch: " + e.getMessage());
 * }
 * finally {
 *     try {
 *         stitch.close();
 *     }
 *     catch (StitchException e) {
 *         System.err.println("Error sending to stitch: " + e.getMessage());
 *     }
 * }
 * }
 * </pre>
 */
public class StitchClient implements Flushable, Closeable {

    // HTTP constants
    public static final String PUSH_URL
        = "https://pipeline-gateway.rjmetrics.com/push";
    private static final int HTTP_CONNECT_TIMEOUT = 1000 * 60 * 2;
    private static final ContentType CONTENT_TYPE =
        ContentType.create("application/transit+json");

    // HTTP properties
    private final int connectTimeout = HTTP_CONNECT_TIMEOUT;
    private final String stitchUrl;

    // Client-specific message values
    private final int clientId;
    private final String token;
    private final String namespace;
    private final String tableName;
    private final List<String> keyNames;

    // Buffer flush time parameters
    private final int batchSizeBytes;
    private final int batchDelayMillis;
    private long lastFlushTime = System.currentTimeMillis();

    private final Buffer buffer;

    private static void putWithDefault(Map map, String key, Object value, Object defaultValue) {
        map.put(key, value != null ? value : defaultValue);
    }

    private static void putIfNotNull(Map map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private Map messageToMap(StitchMessage message) {
        HashMap map = new HashMap();

        switch (message.getAction()) {
        case UPSERT: map.put("action", "upsert"); break;
        case SWITCH_VIEW: map.put("action", "switch_view"); break;
        default: throw new IllegalArgumentException("Action must not be null");
        }

        map.put("client_id", clientId);
        map.put("namespace", namespace);

        putWithDefault(map, "table_name", message.getTableName(), tableName);
        putWithDefault(map, "key_names", message.getKeyNames(), keyNames);

        putIfNotNull(map, "table_version", message.getTableVersion());
        putIfNotNull(map, "sequence", message.getSequence());
        putIfNotNull(map, "data", message.getData());

        return map;
    }

    StitchClient(
        String stitchUrl,
        int clientId,
        String token,
        String namespace,
        String tableName,
        List<String> keyNames,
        int flushIntervalMillis,
        int batchSizeBytes,
        int batchDelayMillis)
    {
        this.stitchUrl = stitchUrl;
        this.clientId = clientId;
        this.token = token;
        this.namespace = namespace;
        this.tableName = tableName;
        this.keyNames = keyNames;
        this.batchSizeBytes = batchSizeBytes;
        this.batchDelayMillis = batchDelayMillis;
        this.buffer = new Buffer();
    }

    private boolean isOverdue() {
        return System.currentTimeMillis() - lastFlushTime  >= batchDelayMillis;
    }

    /**
     * Send a message to Stitch.
     *
     * <p>If buffering is enable (which is true by default), this will
     * will first put the message in an in-memory buffer. Then we
     * check to see if the buffer is full, or if too much time has
     * passed since the last time we flushed. If so, we will deliver
     * all outstanding messages, blocking until the delivery has
     * complited.</p>
     *
     * <p>If you built the StitchClient with buffering disabled (by
     * setting capacity to 0 with {@link
     * StitchClientBuilder#withBufferCapacity}), the message will be sent
     * immediately and this function will block until it is
     * delivered.</p>
     *
     * @throws StitchException if Stitch rejected or was unable to
     *                         process the message
     * @throws IOException if there was an error communicating with
     *                     Stitch
     */
    public void push(StitchMessage message) throws StitchException, IOException {
        buffer.putMessage(messageToMap(message));
        String batch = buffer.takeBatch(this.batchSizeBytes, this.batchDelayMillis);
        if (batch != null) {
            sendBatch(batch);
        }
    }

    private void sendBatch(String batch) throws IOException {
        Request request = Request.Post(stitchUrl)
            .connectTimeout(connectTimeout)
            .addHeader("Authorization", "Bearer " + token)
            .bodyString(batch, CONTENT_TYPE);
        HttpResponse response = request.execute().returnResponse();
        StatusLine statusLine = response.getStatusLine();
        HttpEntity entity = response.getEntity();
        JsonReader rdr = Json.createReader(entity.getContent());
        StitchResponse stitchResponse = new StitchResponse(
            statusLine.getStatusCode(),
            statusLine.getReasonPhrase(),
            rdr.readObject());
        if (!stitchResponse.isOk()) {
            throw new StitchException(stitchResponse);
        }
    }

    /**
     * Send any outstanding messages to Stitch.
     *
     * @throws StitchException if Stitch rejected or was unable to
     *                         process the message
     * @throws IOException if there was an error communicating with
     *                     Stitch
     */
    public void flush() throws IOException {
        while (true) {
            String batch = buffer.takeBatch(0, 0);
            if (batch == null) {
                return;
            }
            sendBatch(batch);
        }
    }

    /**
     * Close the client, flushing all outstanding records to Stitch.
     *
     * @throws StitchException if Stitch rejected or was unable to
     *                         process the message
     * @throws IOException if there was an error communicating with
     *                     Stitch
     */
    public void close() throws IOException {
        flush();
    }
}
