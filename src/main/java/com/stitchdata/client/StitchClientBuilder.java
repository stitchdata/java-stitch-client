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
 * <h3> Optionally tune buffering</h3>
 *
 * By default, every call to {@link StitchClient#push(StitchMessage)}
 * will result in an HTTP request to Stitch. You can get much better
 * performance by allowing the client to accumulate records in its
 * internal buffer. You can do that by specifying the buffer size (up
 * to a max of 4 Mb) with {@link #withBufferCapacity(int)}. If you do
 * this, then a call to {@link StitchClient#push(StitchMessage)} will
 * put the message on an in-memory buffer, and flush the buffer if
 * adding the message causes it to exceed the buffer size
 * limit. Additionally, we'll flush the buffer after a certain period
 * of time (controllable with {@ #withBufferTimeLimit}) even if the
 * buffer is not full.
 *
 * <h3>Full example</h3>
 *
 * <pre>
 * {@code
 * StitchClient stitch = new StitchClientBuilder()
 *
 *   // Required
 *   .withClientId(clientId)
 *   .withToken(token)
 *   .withNamespace(namespace);
 *
 *   // Optionally provide message defaults
 *   .withTableName("order")
 *   .withKeyNames("order_id")
 *
 *   // Allow 1 Mb of records to accumulate in memory
 *   .withBufferCapacity(1000000)
 *
 *   // Flush every 10 seconds
 *   .withBufferTimeLimit(10000)
 *
 *   .build()
 * }
 * </pre>
 */
public class StitchClientBuilder {

    /**
     * By Default flush time limit, see {@link #withBufferTimeLimit}.
     */
    public static final int DEFAULT_FLUSH_INTERVAL_MILLIS = 60000;

    /**
     * By default, each call to {@link StitchClient#push(StitchMessage)} will send to Stitch immediately without buffering.
     */
    public static final int DEFAULT_BUFFER_SIZE = 0;

    /**
     * We can't increase the buffer size larger than 4Mb, because that's the maximum message size Stitch will accept.
     */
    public static final int MAX_BUFFER_SIZE = 4194304;

    private int clientId;
    private String token;
    private String namespace;
    private String tableName;
    private List<String> keyNames;
    private int bufferTimeLimit = DEFAULT_FLUSH_INTERVAL_MILLIS;
    private int bufferCapacity = DEFAULT_BUFFER_SIZE;

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
     * buffer.
     *
     * @param millis time limit in milliseconds
     * @return this object
     */
    public StitchClientBuilder withBufferTimeLimit(int millis) {
        this.bufferTimeLimit = millis;
        return this;
    }

    /**
     * Set the maximum number of bytes we'll accumulate in the buffer
     * before sending a batch of messages to Stitch. When set to 0
     * (the default), we will not buffer messages; every call to
     * {@link StitchClient#push(StitchMessage)} will make a request to
     * Stitch. You can increase this up to 4Mb.
     *
     * @param bytes number of bytes to keep in the buffer
     * @return this object
     */
    public StitchClientBuilder withBufferCapacity(int bytes) {
        this.bufferCapacity = bytes;
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
            bufferTimeLimit,
            bufferCapacity);
    }
}
