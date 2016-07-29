package com.stitchdata.client;

import java.io.ByteArrayOutputStream;
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

    public static final String STITCH_URL =  "https://pipeline-gateway.rjmetrics.com/push";
    private static final ContentType CONTENT_TYPE = ContentType.create("application/transit+json");

    private final int connectTimeout = Stitch.HTTP_CONNECT_TIMEOUT;

    private final String stitchUrl;
    private final int clientId;
    private final String token;
    private final String namespace;

    /**
     * Use this to build instances of StitchClient.
     */
    public static class Builder {
        private int clientId;
        private String token;
        private String namespace;

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

        public StitchClient build() {
            return new StitchClient(STITCH_URL, clientId, token, namespace);
        }
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

}
