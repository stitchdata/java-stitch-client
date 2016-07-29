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
 * Use this to build instances of StitchClient.
 */
public class StitchClientBuilder {

    public static final int DEFAULT_MAX_FLUSH_INTERVAL_MILLIS = 10000;
    public static final int DEFAULT_MAX_FLUSH_BYTES = 4194304;
    public static final int DEFAULT_MAX_FLUSH_RECORDS = 20000;

    private int clientId;
    private String token;
    private String namespace;
    private String tableName;
    private List<String> keyNames;
    private int maxFlushIntervalMillis = DEFAULT_MAX_FLUSH_INTERVAL_MILLIS;
    private int maxBytes = DEFAULT_MAX_FLUSH_BYTES;
    private int maxRecords = DEFAULT_MAX_FLUSH_RECORDS;
    private ResponseHandler responseHandler = null;

    public StitchClientBuilder withClientId(int clientId) {
        this.clientId = clientId;
        return this;
    }

    public StitchClientBuilder withToken(String token) {
        this.token = token;
        return this;
    }

    public StitchClientBuilder withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    public StitchClientBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public StitchClientBuilder withKeyNames(List<String> keyNames) {
        this.keyNames = new ArrayList<String>(keyNames);
        return this;
    }

    public StitchClientBuilder withKeyNames(String... keyNames) {
        return withKeyNames(Arrays.asList(keyNames));
    }

    public StitchClientBuilder withMaxFlushIntervalMillis(int maxFlushIntervalMillis) {
        this.maxFlushIntervalMillis = maxFlushIntervalMillis;
        return this;
    }

    public StitchClientBuilder withMaxBytes(int maxBytes) {
        this.maxBytes = maxBytes;
        return this;
    }

    public StitchClientBuilder withMaxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
        return this;
    }

    public StitchClientBuilder withResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }


    public StitchClient build() {
        return new StitchClientImpl(
            StitchClient.PUSH_URL, clientId, token, namespace,
            tableName, keyNames,
            maxFlushIntervalMillis,
            maxBytes,
            maxRecords,
            responseHandler);
    }
}
