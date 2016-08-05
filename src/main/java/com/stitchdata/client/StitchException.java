package com.stitchdata.client;

import java.io.IOException;

/**
 * Thrown when Stitch cannot accept a message.
 */
public class StitchException extends IOException {

    private final StitchResponse response;

    public StitchException(StitchResponse response, Throwable cause) {
        super(cause);
        this.response = response;
    }

    public StitchException(StitchResponse response) {
        super();
        this.response = response;
    }

    /**
     * Returns the response Stitch gave.
     *
     * @return the response Stitch gave
     */
    public StitchResponse getResponse() {
        return response;
    }

    public String getMessage() {
        return response.toString();
    }
}
