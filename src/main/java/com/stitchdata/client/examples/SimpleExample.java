package com.stitchdata.client.examples;

import com.stitchdata.client.StitchClient;
import com.stitchdata.client.StitchMessage;
import com.stitchdata.client.StitchException;
import java.util.Map;
import java.util.HashMap;

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

        StitchClient client = new StitchClient(clientId, token, namespace);

        Map[] people = new Map[] {
            makePerson(1, "Ben Franklin"),
            makePerson(2, "Betsy Ross"),
            makePerson(3, "Aretha Franklin")
        };

        for (Map person : people) {
            try {
                client.push(client.createMessage()
                            .withAction(StitchMessage.Action.UPSERT)
                            .withTableName("people")
                            .withKeyNames("id")
                            .withSequence(System.currentTimeMillis())
                            .withData(person)
                            .build());
            }
            catch (StitchException e) {
                System.err.println("Error sending message to stitch: " +
                                   e.getMessage());
                System.exit(-1);
            }
        }
    }

}
