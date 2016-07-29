package com.stitchdata.client.examples;

import java.io.IOException;
import com.stitchdata.client.StitchClient;
import com.stitchdata.client.StitchClient.Field;
import com.stitchdata.client.StitchClient.Action;
import com.stitchdata.client.StitchClientBuilder;
import com.stitchdata.client.StitchException;
import com.stitchdata.client.StitchResponse;
import java.util.Arrays;
import java.util.List;
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
            System.err.println("Usage: CLIENT_ID TOKEN NAMESPACE");
            System.exit(-1);
        }

        Integer clientId = Integer.parseInt(args[0]);
        String token = args[1];
        String namespace = args[2];

        StitchClient client = new StitchClientBuilder()
            .withClientId(clientId)
            .withToken(token)
            .withNamespace(namespace)
            .withTableName("people")
            .withKeyNames("id")
            .build();

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
        for (Map person : people) {
            Map message = new HashMap();
            message.put(Field.ACTION, Action.UPSERT);
            message.put(Field.SEQUENCE, sequence);
            message.put(Field.DATA, person);
            messages.add(message);
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
