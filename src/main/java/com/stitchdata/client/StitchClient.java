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
 *   for (Map event : events) {
 *     Map message = new HashMap();
 *     message.set(Stitch.Field.ACTION, Stitch.Action.UPSERT);
 *     message.set(Stitch.Field.SEQUENCE, System.currentTimeMillis());
 *     message.set(Stitch.Field.DATA, event);
 *
 *     stitch.offer(message);
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
 *
 * {@link #offer(List<Map>)}
 * {@link #offer(List<Map>, long, TimeUnit)}
 * {@link #push(List<Map>)}
 * {@link #push(Map)}
 * {@link #put(List<Map>)}
 */
public interface StitchClient extends Closeable {

    /**
     * Attempt to queue a message for delivery without blocking
     *
     * Attempts to queue the message for a background thread to
     * deliver at a later time. Does not block. returns false if the
     * message cannot be queued immediately.
     *
     * @param message the message to queue
     * @return true if the message can be queued immediately, otherwise false.
     */
    public boolean offer(Map message);

    /**
     * Attempt to queue a message for delivery with limited blocking
     *
     * Attempts to queue the message for a background thread to
     * deliver at a later time. Will return after the specified amount
     * of time if we cannot queue the message.
     *
     * @param message the message to queue
     * @return true if the message can be queued within the timeout,
     *         otherwise false.
     */
    public boolean offer(Map m, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Deliver list of messages immediately
     *
     * @param messages List of messages to send
     */
    public StitchResponse push(List<Map> messages) throws StitchException, IOException;

    /**
     * Deliver message immediately
     *
     * @param message messages to send
     */
    public StitchResponse push(Map message) throws StitchException, IOException;

    /**
     * Queue a message for delivery, blocking
     */
    public void put(Map m) throws InterruptedException;

}
