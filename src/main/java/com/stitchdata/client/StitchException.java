package com.stitchdata.client;

public class StitchException extends Exception {

    private final StitchResponse response;

    StitchException(StitchResponse response) {
        this.response = response;
    }

    public StitchResponse getResponse() {
        return response;
    }

    public String getMessage() {
        return response.toString();
    }
}
