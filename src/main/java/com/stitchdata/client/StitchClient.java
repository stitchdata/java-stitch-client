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
 * Client for Stitch.
 *
 * <p> Callers should use {@link StitchClientBuilder} to obtain
 * instances of {@link StitchClient}.  </p>
 *
 * <p> This interface provides several different methods for sending
 * messages to Stith, including synchronous and various asynchronous
 * options. </p>
 *
 * <ul>
 *   <li>{@link #push(Map)} - synchronous, single message</li>
 *   <li>{@link #push(List)} - synchronous, batch of messages</li>
 *   <li>{@link #offer(Map, ResponseHandler)} - asynchronous, non-blocking</li>
 *   <li>{@link #offer(Map, ResponseHandler, long)} - asynchronous, blocking with timeout</li>
 *   <li>{@link #put(Map, ResponseHandler)} - asynchronous, blocking</li>
 * </ul>
 *
 * <p>
 * The synchronous methods ({@link #push(Map)}, {@link #push(List)})
 * block until Stitch accepts the message, or throw {@link
 * StitchException} if it rejects them.
 * </p>
 *
 * <p> The asynchronous methods ({@link #offer(Map, ResponseHandler)},
 * {@link #offer(Map, ResponseHandler, long)}, {@link
 * #put(Map, ResponseHandler)} put the message on an in-memory
 * queue. The messages will be delivered by a background thread. If
 * you want to be notified when a message is delivered or if delivery
 * fails, you can pass in a {@link ResponseHandler}. If you pass in a
 * {@code null} ResponseHandler, you will not be notified after a
 * message is delivered, and failures will be silently ignored. </p>
 */
public interface StitchClient extends Closeable {

    public static final String PUSH_URL
        =  "https://pipeline-gateway.rjmetrics.com/push";

    /**
     * Allowable values for "action" field.
     */
    public static class Action {
        public static final String UPSERT = "upsert";
        public static final String SWITCH_VIEW = "switch_view";
    }

    /**
     * Message field names.
     */
    public static class Field {
        public static final String CLIENT_ID = "client_id";
        public static final String NAMESPACE = "namespace";
        public static final String ACTION = "action";
        public static final String TABLE_NAME = "table_name";
        public static final String TABLE_VERSION = "table_version";
        public static final String KEY_NAMES = "key_names";
        public static final String SEQUENCE = "sequence";
        public static final String DATA = "data";
    }

    /**
     * Attempts to queue a message for later delivery without
     * blocking. Returns false if the message cannot
     * be queued immediately.
     *
     * <p> The caller can optionally provide a {@link ResponseHandler}
     * in order to be notified when the message delivery is completed
     * (or has failed). After the Gate accepts the message, the
     * StitchClient will call {@link ResponseHandler#handleOk(Map,
     * StitchResponse)}.  If delivery fails, it will call {@link
     * ResponseHandler#handleError(Map, Exception)}. </p>
     *
     * @param message the message to queue.
     * @param responseHandler invoked after the delivery is completed or fails.
     * @return <ul>
     *           <li>true - if the message can be queued immediately</li>
     *           <li>false - otherwise</li>
     *         </ul>
     */
    public boolean offer(Map message, ResponseHandler responseHandler);

    /**
     * Attempts to queue a message for later delivery, with a
     * timeout. Will return after the specified amount of time if the
     * message cannot be queued.
     *
     * <p> The caller can optionally provide a {@link ResponseHandler}
     * in order to be notified when the message delivery is completed
     * (or has failed). After the Gate accepts the message, the
     * StitchClient will call {@link ResponseHandler#handleOk(Map,
     * StitchResponse)}.  If delivery fails, it will call {@link
     * ResponseHandler#handleError(Map, Exception)}. </p>
     *
     * @param message the message to queue.
     * @param responseHandler invoked after the delivery is completed or fails.
     * @param timeoutMillis milliseconds to wait for queueing.
     * @return <ul>
     *           <li>true - if the message can be queued immediately</li>
     *           <li>false - otherwise</li>
     *         </ul>
     * @throws InterruptedException if we were interrupted while
     *         trying to queue the message.
     */
    public boolean offer(Map message, ResponseHandler responseHandler,
                         long timeoutMillis)
        throws InterruptedException;

    /**
     * Deliver list of messages immediately.
     *
     * @param messages List of messages to send.
     * @return a {@link StitchResponse} if the push request succeeded
     * @throws StitchException if Stitch  was unable to process the messages.
     * @throws IOException if we had an error communicating with Stitch.
     */
    public StitchResponse push(List<Map> messages)
        throws StitchException, IOException;

    /**
     * Deliver message immediately.
     *
     * @param message message to send.
     * @return a {@link StitchResponse} if the push request succeeded.
     * @throws StitchException if Stitch  was unable to process the message.
     * @throws IOException if we had an error communicating with Stitch.
     */
    public StitchResponse push(Map message) throws StitchException, IOException;

    /**
     * Queue a message for delivery, blocking until we can queue it.
     *
     * <p> The caller can optionally provide a {@link ResponseHandler}
     * in order to be notified when the message delivery is completed
     * (or has failed). After the Gate accepts the message, the
     * StitchClient will call {@link ResponseHandler#handleOk(Map,
     * StitchResponse)}.  If delivery fails, it will call {@link
     * ResponseHandler#handleError(Map, Exception).} </p>
     *
     * @param message the message to queue.
     * @param responseHandler invoked after the delivery is completed or fails.
     * @throws InterruptedException if we were interrupted while
     *         trying to queue the message.
     */
    public void put(Map message, ResponseHandler responseHandler)
        throws InterruptedException;

}
