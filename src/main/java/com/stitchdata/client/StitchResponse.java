package com.stitchdata.client;
import javax.json.JsonReader;
import javax.json.Json;
import javax.json.JsonObject;

/**
 * Encapsulates a response received from Stitch.
 */
public class StitchResponse {
    private final int httpStatusCode;
    private final String httpReasonPhrase;
    private final JsonObject content;

    public StitchResponse(int httpStatusCode, String httpReasonPhrase, JsonObject content) {
        this.httpStatusCode = httpStatusCode;
        this.httpReasonPhrase = httpReasonPhrase;
        this.content = content;
    }

    /**
     * Returns true if the request succeeded.
     *
     * @return <ul>
     *           <li>true - if the request succeeded</li>
     *           <li>false - if the request failed</li>
     *         </ul>
     */
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
        String details = content.toString();
        return "HTTP Status Code " + httpStatusCode +
            " (" + httpReasonPhrase + "): " + details;
    }
}
