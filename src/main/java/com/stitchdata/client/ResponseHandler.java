package com.stitchdata.client;

import java.util.List;

public interface ResponseHandler  {

    public void handleResponse(List<StitchMessage> messages, StitchResponse response);

}
