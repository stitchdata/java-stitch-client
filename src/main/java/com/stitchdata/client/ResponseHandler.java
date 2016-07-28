package com.stitchdata.client;

import java.util.List;
import java.util.Map;

public interface ResponseHandler  {

    public void handleOk(List<Map> messages, StitchResponse response);
    public void handleException(List<Map> messages, Exception exception);

}
