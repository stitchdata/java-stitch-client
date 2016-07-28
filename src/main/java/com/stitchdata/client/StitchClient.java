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
 * Simple client for Stitch.
 */
public class StitchClient {

    public static final String STITCH_URL =  "https://pipeline-gateway.rjmetrics.com/push";
    private static final ContentType CONTENT_TYPE = ContentType.create("application/transit+json");

    private final int connectTimeout = 1000 * 60 * 2;

    private final String stitchUrl;
    private final int clientId;
    private final String token;
    private final String namespace;

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

    public static Builder builder() {
        return new Builder();
    }

    public StitchClient(String stitchUrl, int clientId, String token, String namespace) {
        this.stitchUrl = stitchUrl;
        this.clientId = clientId;
        this.token = token;
        this.namespace = namespace;
    }

    /**
     * Push a list of messages, blocking until Stitch accepts them.
     *
     * @param messages List of messages to send. Use
     *                 client.createMessage() to create messages.
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

    public void validate(Map message) {
        // TODO: Validate
    }

}
