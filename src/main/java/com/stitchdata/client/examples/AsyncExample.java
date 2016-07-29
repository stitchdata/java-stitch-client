package com.stitchdata.client.examples;

import java.io.IOException;
import com.stitchdata.client.Stitch;
import com.stitchdata.client.StitchClient;
import com.stitchdata.client.StitchClientBuilder;
import com.stitchdata.client.StitchException;
import com.stitchdata.client.StitchResponse;
import com.stitchdata.client.ResponseHandler;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class AsyncExample {

    private static Map makePerson(int id, String name) {
        Map result = new HashMap();
        result.put("id", id);
        result.put("name", name);
        return result;
    }

    public static void main(String ...args) {
        if (args.length != 3) {
            System.err.println("Usage: ...");
            System.exit(-1);
        }

        Integer clientId = Integer.parseInt(args[0]);
        String token = args[1];
        String namespace = args[2];

        Map[] people = new Map[] {
            makePerson(1, "Jerry Garcia"),
            makePerson(2, "Omar Rodgriguez Lopez"),
            makePerson(3, "Nina Simone"),
            makePerson(4, "Joni Mitchell"),
            makePerson(5, "David Bowie")
        };

        String tableName = "people";
        List<String> keyNames = Arrays.asList(new String[] { "id" });
        long sequence = System.currentTimeMillis();
        ArrayList<Map> messages = new ArrayList<Map>();

        ResponseHandler responseHandler = new ResponseHandler() {

                public void handleOk(Map message, StitchResponse response) {
                    System.out.println("Got ok: " + response);
                }
                public void handleError(Map message, Exception e) {
                    if (e instanceof StitchException) {
                        System.out.println("Got error response from Stitch: " + e.getMessage() + ((StitchException)e).getResponse().getContent());
                    }
                    else {
                        e.printStackTrace();
                    }

                }
            };

        StitchClient stitch = new StitchClientBuilder()
            .withClientId(clientId)
            .withToken(token)
            .withNamespace(namespace)
            .withTableName("people")
            .withKeyNames("id")
            .withResponseHandler(responseHandler)
            .build();

        try {
            for (Map person : people) {
                Map message = new HashMap();
                message.put(Stitch.Field.ACTION, Stitch.Action.UPSERT);
                message.put(Stitch.Field.SEQUENCE, sequence);
                message.put(Stitch.Field.DATA, person);
                messages.add(message);
                try {
                    stitch.put(message, responseHandler);
                }
                catch (InterruptedException e) {
                    System.err.println("Interrupted while putting record");
                }
            }
        } finally {
            try {
                stitch.close();
            } catch (IOException e) { }
        }
    }

}
