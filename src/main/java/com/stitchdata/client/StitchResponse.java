package com.stitchdata.client;
import javax.json.JsonReader;
import javax.json.Json;
import javax.json.JsonObject;

public class StitchResponse {
    private final int httpStatusCode;
    private final String httpReasonPhrase;
    private final JsonObject content;

    public StitchResponse(int httpStatusCode, String httpReasonPhrase, JsonObject content) {
        this.httpStatusCode = httpStatusCode;
        this.httpReasonPhrase = httpReasonPhrase;
        this.content = content;
    }

    public boolean isOk() {
        return httpStatusCode < 300;
    }

    public int getHttpStatusCode() {
        return httpStatusCode;
    }

    public String getHttpReasonPhrase() {
        return httpReasonPhrase;
    }

    public JsonObject getContent() {
        return content;
    }

    public String toString() {
        if (isOk()) {
        return "HTTP Status Code " + httpStatusCode +
            " (" + httpReasonPhrase + "): " + content.getString("message");
        }
        else {
            return "HTTP Status Code " + httpStatusCode +
                " (" + httpReasonPhrase + "): " + content.getString("error");
        }
    }
}
