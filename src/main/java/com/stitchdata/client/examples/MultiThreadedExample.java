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
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class MultiThreadedExample {

    private static Random random = new Random();

    private static Map randomData(int threadId, int eventNumber) {
        Map result = new HashMap();
        result.put("thread_id", threadId);
        result.put("event_number", eventNumber);
        result.put("random_value", random.nextDouble());
        return result;
    }

    private static class MessageWrapper {
        StitchMessage message = null;
        boolean isEndOfStream = false;
    }

    public static void main(String ...args) {
        if (args.length != 3) {
            System.err.println("Usage: CLIENT_ID TOKEN NAMESPACE");
            System.exit(-1);
        }

        final Integer clientId = Integer.parseInt(args[0]);
        final String token = args[1];
        final String namespace = args[2];
        final BlockingQueue<MessageWrapper> queue = new LinkedBlockingQueue<MessageWrapper>();

        final CountDownLatch latch = new CountDownLatch(1);

        // Start 3 threads pushing messages onto our queue
        for (int i = 0; i < 3; i++) {
            final int threadId = i;
            Thread producer = new Thread() {
                    public void run() {
                        int eventId = 0;
                        try {
                            while (true) {
                                Thread.sleep(random.nextInt(10));
                                MessageWrapper wrapper = new MessageWrapper();
                                wrapper.message = StitchMessage.newUpsert()
                                    .withSequence(System.currentTimeMillis())
                                    .withData(randomData(threadId, eventId++));
                                queue.put(wrapper);
                            }
                        } catch (InterruptedException e) {
                            // do nothing
                        }
                    }
                };
            producer.start();
        }

        // Start 1 thread pulling messages from the queue and sending to Stitch
        Thread consumer = new Thread() {

                StitchClient stitch = new StitchClientBuilder()
                    .withClientId(clientId)
                    .withToken(token)
                    .withNamespace(namespace)
                    .withTableName("multi_threaded_example")
                    .withKeyNames("thread_id", "event_id")
                    .build();

                public void run() {
                    try {
                        MessageWrapper wrapper = queue.take();
                        while (!wrapper.isEndOfStream) {
                            try {
                                stitch.push(wrapper.message);
                            } catch (IOException e) {
                                System.err.println("Error on push: " + e.getMessage());
                            }
                        }
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                    finally {
                        try {
                            stitch.close();
                        } catch (IOException e) {
                            System.err.println("Error on close: " + e.getMessage());
                        } finally {
                            latch.countDown();
                        }
                    }
                }
            };

        consumer.start();
        try {
            Thread.sleep(10000);
            MessageWrapper eos = new MessageWrapper();
            eos.isEndOfStream = true;
            queue.put(eos);
            latch.await();
        } catch (InterruptedException e) {
            // do nothing
        }

    }
}
