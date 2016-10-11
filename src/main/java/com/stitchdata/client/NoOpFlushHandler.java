package com.stitchdata.client;

import java.util.List;

class NoOpFlushHandler implements FlushHandler {
    public void onFlush(List<Object> args) {
        // Don't do anything
    }
};
