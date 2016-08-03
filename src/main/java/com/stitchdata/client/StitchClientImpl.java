package com.stitchdata.client;

import com.stitchdata.client.*;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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

public class StitchClientImpl implements StitchClient {

    private static final ContentType CONTENT_TYPE = ContentType.create("application/transit+json");

    public static final int HTTP_CONNECT_TIMEOUT = 1000 * 60 * 2;
    private final int connectTimeout = HTTP_CONNECT_TIMEOUT;

    private final String stitchUrl;
    private final int clientId;
    private final String token;
    private final String namespace;
    private final String tableName;
    private final List<String> keyNames;
    private final int maxFlushIntervalMillis;
    private final int maxBytes;
    private final int maxRecords;
    private ArrayList<MessageWrapper> buffer = new ArrayList<MessageWrapper>();
    private int numBytes = 0;

    private long lastFlushTime = System.currentTimeMillis();

    private class MessageWrapper {
        byte[] bytes;
    }

    private void flush() throws IOException {

        if (buffer.isEmpty()) {
            return;
        }

        ArrayList<Map> messages = new ArrayList<Map>(buffer.size());
        for (MessageWrapper item : buffer) {
            ByteArrayInputStream bais = new ByteArrayInputStream(item.bytes);
            Reader reader = TransitFactory.reader(TransitFactory.Format.JSON, bais);
            messages.add((Map)reader.read());
        }

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
            if (!stitchResponse.isOk()) {
                throw new StitchException(stitchResponse);
            }
        } catch (ClientProtocolException e) {
            throw new RuntimeException(e);
        }

        buffer.clear();
        numBytes = 0;
        lastFlushTime = System.currentTimeMillis();
    }

    private static void putWithDefault(Map map, String key, Object value, Object defaultValue) {
        map.put(key, value != null ? value : defaultValue);
    }

    private static void putIfNotNull(Map map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private Map messageToMap(StitchMessage message) {
        HashMap map = new HashMap();

        map.put(Field.CLIENT_ID, clientId);
        map.put(Field.NAMESPACE, namespace);

        putWithDefault(map, Field.TABLE_NAME, message.getTableName(), tableName);
        putWithDefault(map, Field.KEY_NAMES, message.getKeyNames(), keyNames);

        putIfNotNull(map, Field.ACTION, message.getAction());
        putIfNotNull(map, Field.TABLE_VERSION, message.getTableVersion());
        putIfNotNull(map, Field.SEQUENCE, message.getSequence());
        putIfNotNull(map, Field.DATA, message.getData());

        return map;
    }

    StitchClientImpl(String stitchUrl, int clientId, String token, String namespace, String tableName, List<String> keyNames,
                     int maxFlushIntervalMillis, int maxBytes, int maxRecords) {
        this.stitchUrl = stitchUrl;
        this.clientId = clientId;
        this.token = token;
        this.namespace = namespace;
        this.tableName = tableName;
        this.keyNames = keyNames;
        this.maxFlushIntervalMillis = maxFlushIntervalMillis;
        this.maxBytes = maxBytes;
        this.maxRecords = maxRecords;
    }

    public void push(StitchMessage message) throws StitchException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MessageWrapper wrapper = new MessageWrapper();
        Writer writer = TransitFactory.writer(TransitFactory.Format.JSON, baos);
        writer.write(messageToMap(message));
        wrapper.bytes = baos.toByteArray();
        buffer.add(wrapper);
        numBytes += wrapper.bytes.length;
        if (numBytes >= maxBytes ||
            buffer.size() >= maxRecords ||
            (System.currentTimeMillis() - lastFlushTime ) >= maxFlushIntervalMillis) {
            flush();
        }
    }

    public void close() throws IOException {
        flush();
    }

}
