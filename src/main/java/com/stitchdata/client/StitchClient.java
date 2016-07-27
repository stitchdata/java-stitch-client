package com.stitchdata.client;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.cognitect.transit.Writer;
import com.cognitect.transit.TransitFactory;



public class StitchClient {

    public static final String STITCH_URL =  "http://pipeline-gateway.rjmetrics.com/push";

    private final String stitchUrl;

    public StitchClient() {
        this(STITCH_URL);
    }

    public StitchClient(String stitchUrl) {
        this.stitchUrl = stitchUrl;
    }

    public void pushMaps(Collection<Map> messages) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = TransitFactory.writer(TransitFactory.Format.MSGPACK, baos);
        writer.write(messages);
    }

    public void push(Collection<StitchMessage> messages) throws StitchException {
        ArrayList<Map> maps = new ArrayList<Map>();
        for (StitchMessage message : messages) {
            maps.add(message.toMap());
        }
        pushMaps(maps);
    }

    public void push(StitchMessage message) throws StitchException {
        ArrayList messages = new ArrayList();
        messages.add(message);
        push(messages);
    }

}
