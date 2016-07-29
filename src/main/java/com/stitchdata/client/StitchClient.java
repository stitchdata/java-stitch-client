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
public interface StitchClient extends Closeable {

    public boolean offer(Map m);

    public boolean offer(Map m, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Push a list of messages, blocking until Stitch accepts them.
     *
     * @param messages List of messages to send
     */
    public StitchResponse push(List<Map> messages) throws StitchException, IOException;

    /**
     * Push a single message, blocking until Stitch accepts it.
     *
     * @param message messages to send
     */
    public StitchResponse push(Map message) throws StitchException, IOException;

    public void put(Map m) throws InterruptedException;

    public void validate(Map message);

}
