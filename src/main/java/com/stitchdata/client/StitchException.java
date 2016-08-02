package com.stitchdata.client;

import java.io.IOException;

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

    public StitchResponse getResponse() {
        return response;
    }

    public String getMessage() {
        return response.toString();
    }
}
