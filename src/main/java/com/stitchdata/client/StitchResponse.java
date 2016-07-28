package com.stitchdata.client;

public class StitchResponse {
    private final int httpStatusCode;
    private final String httpReasonPhrase;
    private final JsonObject content;

    public StitchResponse(int httpStatusCode, String httpReasonPhrase, JsonObject content) {
        this.httpStatusCode = httpStatusCode;
        this.httpReasonPhrase = httpReasonPhrase;
        this.content = content;
    }

    public int isOk() {
        return httpStatusCode < 300;
    }

    public String getHttpStatusCode() {
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
        return "HTTP Response Code " + httpResponseCode +
            " (" + httpReasonPhrase + "): " + content.getString("message");
        }
        else {
            return "HTTP Response Code " + httpResponseCode +
                " (" + httpReasonPhrase + "): " + content.getString("error");
        }
    }
}
