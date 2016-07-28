package com.stitchdata.client;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

public class StitchClient {

    public static final String STITCH_URL =  "https://pipeline-gateway.rjmetrics.com/push";
    private static final ContentType CONTENT_TYPE = ContentType.create("application/transit+json");

    private final int connectTimeout = 1000 * 60 * 2;

    private final String stitchUrl;
    private final int clientId;
    private final String token;
    private final String namespace;

    public StitchClient(int clientId, String token, String namespace) {
        this(STITCH_URL, clientId, token, namespace);
    }

    public StitchClient(String stitchUrl, int clientId, String token, String namespace) {
        this.stitchUrl = stitchUrl;
        this.clientId = clientId;
        this.token = token;
        this.namespace = namespace;
    }

    public StitchMessage createMessage() {
        return new StitchMessage(clientId, token, namespace);
    }

    public StitchResponse push(List<Map> messages) throws StitchException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messages);
        String body = null;
        try {
            body = baos.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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
            if (statusLine.getStatusCode() >= 300) {
                throw new StitchException(stitchResponse);
            }
            else {
                return stitchResponse;
            }
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void push(Map message) throws StitchException {
        ArrayList<Map> messages = new ArrayList<Map>();
        messages.add(message);
        push(messages);
    }

}
