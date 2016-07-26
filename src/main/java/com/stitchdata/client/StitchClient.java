package com.stitchdata.client;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.ArrayList;
import com.cognitect.transit.Writer;
import com.cognitect.transit.TransitFactory;

public class StitchClient {

    public static final String STITCH_URL =  "http://pipeline-gateway.rjmetrics.com/push";

    private final String url;

    public StitchClient() {
        this(STITCH_URL);
    }

    public StitchClient(String url) {
        this.url = url;
    }

    public void push(Collection<StitchMessage> messages) throws StitchException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.MSGPACK, baos);
        ArrayList data = new ArrayList();
        for (StitchMessage message : messages) {
            data.add(message.toMap());
        }
        writer.write(data);
    }

    public void push(StitchMessage message) throws StitchException {
        ArrayList messages = new ArrayList();
        messages.add(message);
        push(messages);
    }

}
