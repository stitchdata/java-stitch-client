Quick Start
===========

Building a Client
-----------------

Use StitchClientBuilder to build a stitch client. You'll
need to set your client id, authentication token, and namespace,
all of which are available at http://stitchdata.com.

```java
StitchClient stitch = new StitchClientBuilder()
  .withClientId(yourClientId)
  .withToken(yourToken)
  .withNamespace(yourNamespace)
  .build();
```

Building a Message
------------------

You can build a Stitch message by creating a new instance of
StitchMessage and then calling methods on it to set the properties of
the message. For example:

```java
StitchMessage message = new StitchMessage()
    .withAction(Action.UPSERT)
    .withTableName("events")
    .withKeyName("event_id")
    .withSequence(System.currentTimeMillis())
    .withData(data);
```

In a typical use case, several of the fields will be the same for all
messages that you send using a single client. To make this use case
more convenient, you can set some of those fields on the client using
`StitchClientBuilder`. The resulting client will inject the values for
those fields into every message it sends.

```java
StitchClient stitch = new StitchClientBuilder()
  .withClientId(yourClientId)
  .withToken(yourToken)
  .withNamespace(yourNamespace)
  .withTableName("events")
  .withKeyNames("id")
  .build();

// I can omit the table name and key names:
StitchMessage message = new StitchMessage()
    .withAction(Action.UPSERT)
    .withSequence(System.currentTimeMillis())
    .withData(data);
```

Sending Messages
----------------

You send messages to Stitch by calling

```
try {
    stitch.push(message);
}
catch (StitchException e) {
    System.err.println("Error sending message to Stitch");
}
```

### Synchronous Delivery

`push(Map message)` sends a single record and returns a
`StitchResponse`. It throws `StitchException` if stitch rejects the
record. There is typically no reason to inspect the StitchResponse -- if
`push` doesn't throw, that means Stitch accepted the record.

```java
try {
  stitch.push(message);
} catch (StitchException e) {
  // Handle error...
}
```

`push(List<Map> messages)` sends a list of records at once. Note that
there are limitations for the number of messages and total size of
request that Stitch will accept. If you exceed these limits, you'll
get a `StitchException`. It's probably easier to use the asynchronous
methods, which will ensure that the requests satisfy the limits.

### Asynchronous Delivery

The asynchronous methods each take a single message and put it on an
in-memory queue, where a background thread will pick it up and deliver
it later. All of the asynchronous methods accept an optional
`responseHandler` argument, which the background thread will call
after the message has been delivered, or if the delivery fails.

`put(Map message, ResponseHandler responseHandler)` puts a record onto
the queue, blocking if there is no room in the queue. `offer(Map
message, ResponseHandler responseHandler)` is a non-blocking
version. If the message can be queued, `offer` will queue it and
return true immediately. If not, it will return false. `offer(Map
message, ResponseHandler responseHandler, long timeout)` will block
for up to the specified number of milliseconds attempting to queue the
record.

#### Creating a Response Handler

```java
ResponseHandler hander = new ResponseHandler() {

    void handleOk(Map message, StitchResponse response) {
        log.debug("Delivered a message");
    }
    void handleError(Map message, StitchException exception) {
        log.error(exception, "Error sending message ");
    }
}
```

#### Blocking

```java
try {
    stitch.put(message, responseHandler);
} catch (InterruptedException e) {
    log.warn(e, "Interrupted while queueing message");
}
```

#### Non-Blocking

```java
boolean queued = stitch.offer(message, responseHandler);
if (!queued) {
    log.warn("Queue is full");
}
```

#### With Timeout

```java
try {
    boolean queued = stitch.offer(message, responseHandler, 10000);
    if (!queued) {
        log.warn("Unable to queue message within 10 seconds");
    }
} catch (InterruptedException e) {
    log.warn(e, "Interrupted while queueing message");
}
```

#### Tuning

If you will be using the asynchronous delivery methods, and you want
finer control over how frequently the background thread sends
messages, there are several methods for adjusting those parameters:

```java
StitchClient stitch = new StitchClientBuilder()
  .withClientId(yourClientId)
  .withToken(yourToken)
  .withNamespace(yourNamespace)
  .withMaxFlushIntervalMillis(60000) // Flush at least once a minute
  .withMaxBytes(1000000) // Flush when we hit 1 Mb of serialized data
  .withMaxRecords(100) // Flush when we hit 100 records
  .build();
```
