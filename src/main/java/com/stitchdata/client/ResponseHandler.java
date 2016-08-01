package com.stitchdata.client;

import java.util.List;
import java.util.Map;

/**
 * Callback for receiving notifications after message delivery
 * succeeds or fails, for use with asynchronous delivery
 * methods.
 *
 * <p><em>Note: These methods will be called by the background thread that
 * delivers messages to stitch, so implementations should take care to
 * avoid blocking or io-intensive operations.</em></p>
 *
 * <h3>Example: Logging responses</h3>
 *
 * <pre>
 * {@code
 * public class LoggingResponseHandler implements ResponseHandler {
 *   public void handleOk(Map message, StitchResponse response) {
 *     log.debug("Message delivery succeeded" + response.toString());
 *   }
 *   public void handleError(Map message, Exception exception) {
 *     log.debug("Message delivery failed" + exception.getMessage());
 *   }
 * }
 * }
 * </pre>
 */
public interface ResponseHandler  {

    /**
     * Called after Stitch accepts the message.
     *
     * <p><em>Note: This will be called by the background thread that
     * delivers messages to stitch, so implementations should take
     * care to avoid blocking or io-intensive operations.</em></p>
     *
     * @param message the message that was accepted.
     * @param response the response that Stitch returned.
     */
    public void handleOk(Map message, StitchResponse response);

    /**
     * Called by the background thread after it tried but failed to
     * deliver a message to Stitch. Implementations may want to
     * inspect the type of the exception. A StitchException indicates
     * that we connected to Stitch, but the service was unable to
     * handle the record for some reason. Any other type of exception
     * indicates that we were unable to talk to Stitch.
     *
     * <p><em>Note: This will be called by the background thread that
     * delivers messages to stitch, so implementations should take
     * care to avoid blocking or io-intensive operations.</em></p>
     *
     * @param message the message that we were unable to deliver.
     * @param exception the exception that was generated when we tried to deliver the message.
     */
    public void handleError(Map message, Exception exception);

}
