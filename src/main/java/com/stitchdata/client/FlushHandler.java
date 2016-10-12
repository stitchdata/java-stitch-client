package com.stitchdata.client;

import java.util.List;

/**
 * Use this to be notified when records in the buffer are flushed to
 * Stitch.
 */
public interface FlushHandler {

    /**
     * Called after a successful flush, with the list of callbackArgs
     * corresponding to the records that were flushed.
     */
    public void onFlush(List callbackArgs);
}
