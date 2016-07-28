package com.stitchdata.client.examples;

import java.io.IOException;
import com.stitchdata.client.StitchClient;
import com.stitchdata.client.StitchMessage;
import com.stitchdata.client.StitchException;
import com.stitchdata.client.StitchResponse;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

public class SimpleExample {

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

        StitchClient client = new StitchClient.Builder()
            .withClientId(clientId)
            .withToken(token)
            .withNamespace(namespace)
            .build();

        Map[] people = new Map[] {
            makePerson(1, "Ben Franklin"),
            makePerson(2, "Betsy Ross"),
            makePerson(3, "Aretha Franklin")
        };

        ArrayList<Map> messages = new ArrayList<Map>();
        for (Map person : people) {
            messages.add(client.createMessage()
                                .withAction(StitchMessage.Action.UPSERT)
                                .withTableName("people")
                                .withKeyNames("id")
                                .withSequence(System.currentTimeMillis())
                                .withData(person)
                                .build());
        }
        try {
            StitchResponse response = client.push(messages);
            System.out.println(response);
        }
        catch (StitchException e) {
            System.out.println("Got error response from Stitch: " + e.getMessage() + ((StitchException)e).getResponse().getContent());
            System.exit(-1);
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}
