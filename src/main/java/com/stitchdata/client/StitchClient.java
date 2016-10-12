package com.stitchdata.client;

import java.io.Closeable;
import java.io.EOFException;
import java.io.Flushable;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

import java.util.List;
import java.util.ArrayList;
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
import com.cognitect.transit.Writer;
import com.cognitect.transit.WriteHandler;
import com.cognitect.transit.TransitFactory;
import com.cognitect.transit.Reader;

/**
 * Client for Stitch.
 *
 * <p>Callers should use {@link StitchClientBuilder} to construct
 * instances of {@link StitchClient}.</p>
 *
 * A StitchClient takes messages (instances of {@link StitchMessage})
 * and submits them to Stitch in batches. A call to {@link
 * StitchClient#push(StitchMessage)} adds a message to the current
 * batch, and then either delivers the batch immediately or waits
 * until we accumulate more messages. By default, StitchClient will
 * send a batch when it has accumulated 4 Mb of data or when 60
 * seconds have passed since the last batch was sent. These parameters
 * can be configured with {@link
 * StitchClientBuilder#withBatchSizeBytes(int)} and {@link
 * StitchClientBuilder#withBatchDelayMillis(int)}. Setting
 * batchSizeBytes to 0 will effectively disable batching and cause
 * each call to {@link #push(StitchMessage)} to send the message
 * immediatley.
 *
 * You should open the client in a try-with-resources statement to
 * ensure that it is closed, otherwise you will lose any messages that
 * have been added to the buffer but not yet delivered.
 *
 * <pre>
 * {@code
 * try (StitchClient stitch = new StitchClientBuilder()
 *   .withClientId(123)
 *   .withToken("asdfasdfasdfasdasdfasdfadsfadfasdfasdfadfsasdf")
 *   .withNamespace("event_tracking")
 *   .withTableName("events")
 *   .withKeyNames(thePrimaryKeyFields)
 *   .build())
 * {
 *     for (Map data : someSourceOfRecords) {
 *         stitch.push(StitchMessage.newUpsert()
 *             .withSequence(System.currentTimeMillis())
 *             .withData(data));
 *     }
 * }
 * catch (StitchException e) {
 *     System.err.println("Error sending to stitch: " + e.getMessage());
 * }
 * }
 * </pre>
 *
 * Instances of StitchClient are thread-safe. If buffering is enabled
 * (which it is by default), then multiple threads will accumulate
 * records into the same batch. When one of those threads makes a call
 * to {@link #push(StitchMessage)} that causes the buffer to fill up,
 * that thread will deliver the entire batch to Stitch. This behavior
 * should be suitable for many applications. However, if you do not
 * want records from multiple threads to be sent on the same batch, or
 * if you want to ensure that a record is only delivered by the thread
 * that produced it, then you can create a separate StitchClient for
 * each thread.
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
    private final FlushHandler flushHandler;
    private final Map<Class,WriteHandler<?,?>> writeHandlers;

    private static void putWithDefault(Map map, String key, Object value, Object defaultValue) {
        map.put(key, value != null ? value : defaultValue);
    }

    private static void putIfNotNull(Map map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }




    private byte[] messageToBytes(StitchMessage message) {
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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos, writeHandlers);
        writer.write(map);
        return baos.toByteArray();
    }

    StitchClient(
        String stitchUrl,
        int clientId,
        String token,
        String namespace,
        String tableName,
        List<String> keyNames,
        int batchSizeBytes,
        int batchDelayMillis,
        FlushHandler flushHandler,
        Map<Class,WriteHandler<?,?>> writeHandlers)
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
        this.flushHandler = flushHandler;
        this.writeHandlers = TransitFactory.writeHandlerMap(writeHandlers);
    }

    /**
     * Send a message to Stitch.
     *
     * <p>Adds the message to the current batch, sending the batch if
     * we have accumulated enough data.</p>
     *
     * <p>If you built the StitchClient with batching disabled (by
     * setting batchSizeBytes to 0 with {@link
     * StitchClientBuilder#withBatchSizeBytes}), the message will be
     * sent immediately and this function will block until it is
     * delivered.</p>
     *
     * @throws StitchException if Stitch rejected or was unable to
     *                         process the message
     * @throws IOException if there was an error communicating with
     *                     Stitch
     */
    public void push(StitchMessage message) throws StitchException, IOException {
        push(message, message);
    }

    /**
     * Send a message to Stitch.
     *
     * <p>Adds the message to the current batch, sending the batch if
     * we have accumulated enough data. Callers that wish to be
     * notified after a record has been accepted by the gate should
     * register a FlushHandler when initializing the client, and then
     * provide a callbackArg to this function. After every successful
     * flush we'll call the FlushHandler for this client, passing in
     * the list of callbackArgs that correspond to the records flushed
     * n this batch.</p>
     *
     * <p>If you built the StitchClient with batching disabled (by
     * setting batchSizeBytes to 0 with {@link
     * StitchClientBuilder#withBatchSizeBytes}), the message will be
     * sent immediately and this function will block until it is
     * delivered.</p>
     *
     * @throws StitchException if Stitch rejected or was unable to
     *                         process the message
     * @throws IOException if there was an error communicating with
     *                     Stitch
     */
    public void push(StitchMessage message, Object callbackArg) throws StitchException, IOException {
        buffer.put(new Buffer.Entry(messageToBytes(message), callbackArg));
        List<Buffer.Entry> batch = buffer.take(this.batchSizeBytes, this.batchDelayMillis);
        if (batch != null) {
            sendBatch(batch);
        }
    }


    StitchResponse sendToStitch(String body) throws IOException {
        Request request = Request.Post(stitchUrl)
            .connectTimeout(connectTimeout)
            .addHeader("Authorization", "Bearer " + token)
            .bodyString(body, CONTENT_TYPE);
        HttpResponse response = request.execute().returnResponse();
        StatusLine statusLine = response.getStatusLine();
        HttpEntity entity = response.getEntity();
        JsonReader rdr = Json.createReader(entity.getContent());
        return new StitchResponse(statusLine.getStatusCode(),
                                  statusLine.getReasonPhrase(),
                                  rdr.readObject());
    }

    void sendBatch(List<Buffer.Entry> batch) throws IOException {

        String body = serializeEntries(batch);

        StitchResponse stitchResponse = sendToStitch(body);

        if (!stitchResponse.isOk()) {
            throw new StitchException(stitchResponse);
        }

        if (flushHandler != null) {
            ArrayList callbackArgs = new ArrayList();
            for (Buffer.Entry entry : batch) {
                callbackArgs.add(entry.callbackArg);
            }
            flushHandler.onFlush(callbackArgs);
        }
    }

    static String serializeEntries(List<Buffer.Entry> entries) throws UnsupportedEncodingException {
        if (entries == null) {
            return null;
        }

        ArrayList<Map> messages = new ArrayList<Map>();

        for (Buffer.Entry entry : entries) {
            ByteArrayInputStream bais = new ByteArrayInputStream(entry.bytes);
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            messages.add((Map)reader.read());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messages);
        return baos.toString("UTF-8");
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
            List<Buffer.Entry> batch = buffer.take(0, 0);
            if (batch == null) {
                return;
            }
            sendBatch(batch);
        }
    }

    /**
     * Close the client, flushing all outstanding messages to Stitch.
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
