package com.stitchdata.client;

import java.util.List;
import java.util.Map;

public interface ResponseHandler  {

    public void handleOk(Map messages, StitchResponse response);
    public void handleError(Map messages, Exception exception);

}
