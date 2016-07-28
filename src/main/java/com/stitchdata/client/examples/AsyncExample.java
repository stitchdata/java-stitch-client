package com.stitchdata.client.examples;

import java.io.IOException;
import com.stitchdata.client.AsyncStitchClient;
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

                public void handleOk(List<Map> messages, StitchResponse response) {
                    System.out.println("Got ok: " + response);
                }
                public void handleError(List<Map> messages, Exception e) {
                    if (e instanceof StitchException) {
                        System.out.println("Got error response from Stitch: " + e.getMessage() + ((StitchException)e).getResponse().getContent());
                    }
                    else {
                        e.printStackTrace();
                    }

                }
            };

        AsyncStitchClient client = AsyncStitchClient.builder()
            .withClientId(clientId)
            .withToken(token)
            .withNamespace(namespace)
            .withResponseHandler(responseHandler)
            .build();

        try {
            for (Map person : people) {
                try {
                    client.put(client.newUpsertMessage(tableName, keyNames, sequence, person));
                }
                catch (InterruptedException e) {
                    System.err.println("Interrupted while putting record");
                }
            }
        } finally {
            client.close();
        }
    }

}
