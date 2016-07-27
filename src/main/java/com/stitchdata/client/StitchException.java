package com.stitchdata.client;

import javax.json.JsonObject;

public class StitchException extends Exception {

    private final int httpResponseCode;
    private final String httpReasonPhrase;
    private final JsonObject content;

    StitchException(int httpResponseCode, String httpReasonPhrase, JsonObject content) {
        this.httpResponseCode = httpResponseCode;
        this.httpReasonPhrase = httpReasonPhrase;
        this.content = content;
    }

    public String getMessage() {
        return "Error sending message to gate. HTTP Response Code " + httpResponseCode +
            " (" + httpReasonPhrase + "): " + content.getString("error");
    }
}
