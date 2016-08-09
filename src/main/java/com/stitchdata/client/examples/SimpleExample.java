package com.stitchdata.client.examples;

import java.io.IOException;
import com.stitchdata.client.StitchClient;
import com.stitchdata.client.StitchClientBuilder;
import com.stitchdata.client.StitchException;
import com.stitchdata.client.StitchMessage;
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

    public static void exitWithError(String message) {
        System.err.println(message);
        System.exit(-1);
    }

    public static void main(String ...args) {
        if (args.length != 3) {
            System.err.println("Usage: CLIENT_ID TOKEN NAMESPACE");
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

        try (StitchClient stitch = new StitchClientBuilder()
             .withClientId(clientId)
             .withToken(token)
             .withNamespace(namespace)
             .withTableName("people")
             .withKeyNames("id")
             .build()) {
            for (Map person : people) {
                stitch.push(
                    StitchMessage.newUpsert()
                    .withSequence(System.currentTimeMillis())
                    .withData(person));
            }
        }
        catch (StitchException e) {
            exitWithError("Stitch error " + e.getMessage());
        }
        catch (IOException e) {
            exitWithError(e.getMessage());
        }
    }
}
