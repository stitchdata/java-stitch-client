package com.stitchdata.client;

import java.util.List;

public interface FlushHandler {
    public void onFlush(List<Object> identifiers); // implicit success
}
