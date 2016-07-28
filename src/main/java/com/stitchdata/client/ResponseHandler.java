package com.stitchdata.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.ArrayList;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.cognitect.transit.Writer;
import com.cognitect.transit.Reader;
import com.cognitect.transit.TransitFactory;

public interface ResponseHandler  {

    public void handleResponse(List<Map> messages, StitchResponse response);

}
