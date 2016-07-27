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

public class StitchClient {

    public static final String STITCH_URL =  "https://pipeline-gateway.rjmetrics.com/push";
    private static final ContentType CONTENT_TYPE = ContentType.create("application/transit+json");

    private final int connectTimeout = 1000 * 60 * 2;

    private final String stitchUrl;
    private final int clientId;
    private final String token;

    public StitchClient(int clientId, String token) {
        this(STITCH_URL, clientId, token);
    }

    public StitchClient(String stitchUrl, int clientId, String token) {
        this.stitchUrl = stitchUrl;
        this.clientId = clientId;
        this.token = token;
    }

    public void pushMaps(Collection<Map> messages) {
        for (Map m : messages) {
            m.put("client_id", clientId);
        }
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
            System.out.println("Status line " + statusLine);
            BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void push(Collection<StitchMessage> messages) throws StitchException {
        ArrayList<Map> maps = new ArrayList<Map>();
        for (StitchMessage message : messages) {
            maps.add(message.toMap());
        }
        pushMaps(maps);
    }

    public void push(StitchMessage message) throws StitchException {
        ArrayList messages = new ArrayList();
        messages.add(message);
        push(messages);
    }

}
