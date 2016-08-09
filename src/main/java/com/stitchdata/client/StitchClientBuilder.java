package com.stitchdata.client;

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
 * Use this to build instances of StitchClient.
 *
 * <h3>Basic usage</h3>
 *
 * Every client must have a client id, access token, and
 * namespace. You should have gotten these parameters when you set up
 * the integration at http://stitchdata.com. You must set them with
 * {@link #withClientId(int)}, {@link #withToken(String)}, and {@link
 * #withNamespace(String)}.
 *
 * <pre>
 * {@code
 * StitchClient stitch = new StitchClientBuilder()
 *   .withClientId(123)
 *   .withToken("asdfasdfasdfasdfasdfasdfasdfasdfasdf")
 *   .withNamespace("event_tracking")
 *   .build();
 * }
 * </pre>
 *
 * <h3>Optionally set message defaults</h3>
 *
 * If your application will send messages into only one table, you can
 * set the table name and key names here with {@link
 * #withTableName(String)} and {@link #withKeyNames(List)}, {@link
 * #withKeyNames(String...)}, or {@link #withKeyName(String)}. If you
 * set those values here, you don't need to set them in the messages
 * you send through this client. If you don't set them here, you must
 * set them on the messages.
 *
 * <pre>
 * {@code
 * StitchClient stitch = new StitchClientBuilder()
 *   .withClientId(123)
 *   .withToken("asdfasdfasdfasdfasdfasdfasdfasdfasdf")
 *   .withNamespace("event_tracking")
 *   .withTableName("events")
 *   .withKeyNames("hostname", "threadId", "timestamp")
 *   .build();
 * }
 * </pre>
 *
 * <h3> Optionally tune batch parameters</h3>
 *
 * A StitchClient takes records (instances of {@link StitchMessage})
 * and submits them to Stitch in batches. A call to {@link
 * StitchClient#push(StitchMessage)} adds a record to the current
 * batch, and then either delivers the batch immediately or waits
 * until we accumulate more records. By default, StitchClient will
 * send a batch when it has accumulated 4 Mb of data or when 60
 * seconds have passed since the last batch was sent. These parameters
 * can be configured with {@link
 * StitchClientBuilder#withBatchSizeBytes(int)} and {@link
 * StitchClientBuilder#withBatchDelayMillis(int)}. Setting
 * batchSizeBytes to 0 will effectively disable batching and cause
 * each call to {@link #StitchClient.push(StitchMessage)} to send the
 * record immediatley.
 *
 * <pre>
 * {@code
 * try (StitchClient stitch = new StitchClientBuilder()
 *     .withClientId(clientId)
 *     .withToken(token)
 *     .withNamespace(namespace);
 *
 *     // Allow 1 Mb of records to accumulate in memory
 *     .withBatchSizeBytes(1000000)
 *
 *     // Flush every 10 seconds
 *     .withBatchDelayMillis(10000)
 *
 *     .build())
 * {
 *     // ...
 * }
 * }
 * </pre>
 */
public class StitchClientBuilder {

    /**
     * By default, {@link StitchClient#push(StitchMessage)} will send
     * a batch if it hasn't sent a batch in more than a minute.
     */
    public static final int DEFAULT_BATCH_DELAY_MILLIS = 60000;

    /**
     * By default, {@link StitchClient#push(StitchMessage)} will send
     * a batch if it has reached 4 Mb.
     */
    public static final int DEFAULT_BATCH_SIZE_BYTES = 4194304;

    private int clientId;
    private String token;
    private String namespace;
    private String tableName;
    private List<String> keyNames;
    private int batchSizeBytes = DEFAULT_BATCH_SIZE_BYTES;
    private int batchDelayMillis = DEFAULT_BATCH_DELAY_MILLIS;

    /**
     * Specify your Stitch client id. This is a required setting.
     *
     * @param clientId the client id
     * @return this object
     */
    public StitchClientBuilder withClientId(int clientId) {
        this.clientId = clientId;
        return this;
    }

    /**
     * Specify your Stitch access token. This is a required setting.
     *
     * @param token the access token
     * @return this object
     */
    public StitchClientBuilder withToken(String token) {
        this.token = token;
        return this;
    }

    /**
     * Specify the namespace, which you can find at
     * http://stitchdata.com. This is a required setting.
     *
     * @param namespace the namespace
     * @return this object
     */
    public StitchClientBuilder withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    /**
     * Set the name of the table. This is an optional setting. If all
     * the messages you'll be sending with this client are going to
     * the same table, you can set it here. Otherwise, you can set it
     * individually on each message with {@link
     * StitchMessage#withTableName}.
     *
     * @param tableName the table name
     * @return this object
     */
    public StitchClientBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * Set the key names. This is an optional setting. If all the
     * messages you'll be sending with this client are going to the
     * same table, you can set the key names here. Otherwise, you can
     * set them individually on each message with {@link
     * StitchMessage#withKeyNames}.
     *
     * @param keyNames list of key names
     * @return this object
     */
    public StitchClientBuilder withKeyNames(List<String> keyNames) {
        this.keyNames = new ArrayList<String>(keyNames);
        return this;
    }

    /**
     * Set the key names. This is an optional setting. If all the
     * messages you'll be sending with this client are going to the
     * same table, you can set the key names here. Otherwise, you can
     * set them individually on each message with {@link
     * StitchMessage#withKeyNames}.
     *
     * @param keyNames array of key names
     * @return this object
     */
    public StitchClientBuilder withKeyNames(String... keyNames) {
        return withKeyNames(Arrays.asList(keyNames));
    }

    /**
     * Set the key name, for use when there is just one key name. This
     * is an optional setting. If all the messages you'll be sending
     * with this client are going to the same table, you can set the
     * key name here. Otherwise, you can set it individually on each
     * message with {@link StitchMessage#withKeyNames}.
     *
     * @param keyName  key names
     * @return this object
     */
    public StitchClientBuilder withKeyName(String keyName) {
        return withKeyNames(keyName);
    }

    /**
     * Set the limit for the amount of time we'll leave records in the
     * batch without sending.
     *
     * @param millis time limit in milliseconds
     * @return this object
     */
    public StitchClientBuilder withBatchDelayMillis(int millis) {
        this.batchDelayMillis = millis;
        return this;
    }

    /**
     * Set the maximum number of bytes we'll accumulate before sending
     * a batch of messages to Stitch. When set to 0, we will not
     * buffer messages; every call to {@link
     * StitchClient#push(StitchMessage)} will make a request to
     * Stitch. You can increase this up to 4Mb.
     *
     * @param bytes number of bytes to keep in the buffer
     * @return this object
     */
    public StitchClientBuilder withBatchSizeBytes(int bytes) {
        this.batchSizeBytes = bytes;
        return this;
    }

    /**
     * Return a new StitchClient.
     *
     * @return a new StitchClient
     */
    public StitchClient build() {
        return new StitchClient(
            StitchClient.PUSH_URL, clientId, token, namespace,
            tableName, keyNames,
            batchSizeBytes,
            batchDelayMillis);
    }
}
