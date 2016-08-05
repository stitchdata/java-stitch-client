package com.stitchdata.client;

import com.stitchdata.client.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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

    public static final String PUSH_URL
        = "https://pipeline-gateway.rjmetrics.com/push";

    private static final int HTTP_CONNECT_TIMEOUT = 1000 * 60 * 2;
    private static final ContentType CONTENT_TYPE =
        ContentType.create("application/transit+json");

    private final int connectTimeout = HTTP_CONNECT_TIMEOUT;
    private final String stitchUrl;
    private final int clientId;
    private final String token;
    private final String namespace;
    private final String tableName;
    private final List<String> keyNames;
    private final int flushIntervalMillis;
    private final int bufferCapacity;
    private int bufferSize = 0;

    private long lastFlushTime = System.currentTimeMillis();

    private class ByteArrayWrapper {
        byte[] bytes;
    }

    private final List<ByteArrayWrapper> buffer = new ArrayList<ByteArrayWrapper>();

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
        int bufferCapacity)
    {
        this.stitchUrl = stitchUrl;
        this.clientId = clientId;
        this.token = token;
        this.namespace = namespace;
        this.tableName = tableName;
        this.keyNames = keyNames;
        this.flushIntervalMillis = flushIntervalMillis;
        this.bufferCapacity = bufferCapacity;
    }

    private boolean isBufferFull() {
        return bufferSize >= bufferCapacity;
    }

    private boolean isOverdue() {
        return System.currentTimeMillis() - lastFlushTime  >= flushIntervalMillis;
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messageToMap(message));
        ByteArrayWrapper wrapper = new ByteArrayWrapper();
        wrapper.bytes = baos.toByteArray();
        bufferSize++;

        if (isBufferFull() || isOverdue()) {
            flush();
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

        if (buffer.isEmpty()) {
            return;
        }

        ArrayList<Map> messages = new ArrayList<Map>(buffer.size());
        for (ByteArrayWrapper wrapper : buffer) {
            ByteArrayInputStream bais = new ByteArrayInputStream(wrapper.bytes);
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            messages.add((Map)reader.read());
        }

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
            if (!stitchResponse.isOk()) {
                throw new StitchException(stitchResponse);
            }
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        }

        buffer.clear();
        bufferSize = 0;
        lastFlushTime = System.currentTimeMillis();
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
