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

If you will be using this clients to push records into a single
table, you may find it convenient to specify the table and key
names in the client.

```java
StitchClient stitch = new StitchClientBuilder()
  // ...
  .withTableName("events")
  .withKeyNames("id")
  .build();
```

If you will be using the asynchronous delivery methods, and you want
finer control over how frequently the background thread sends
messages, there are several methods for adjusting those parameters:

```java
StitchClient stitch = new StitchClientBuilder()
  // ...
  .withMaxFlushIntervalMillis(60000) // Flush at least once a minute
  .withMaxBytes(1000000) // Flush when we hit 1 Mb of serialized data
  .withMaxRecords(100) // Flush when we hit 100 records
  .build();
```

Building a Message
------------------

Every stitch message is a map. The allowed field names of the map are
listed in `StitchClient.Field`:

* "client_id" - Your client identifier, obtained from http://stitchdata.com
* "namespace" - The name you gave to the integration at http://stitchdata.com
* "action" - The action to perform. Currently the only supported action is "upsert".
* upsert fields:
  * "table_name" - The name of the table to upsert records into.
  * "key_names" - List of keys into data map that identify the record.
  * "sequence" - Sequence number that will be used to impose ordering of updates to a single record.
  * "data" - The record to upsert, as a map.

```java
Map message = new HashMap();
message.put(Field.CLIENT_ID, 1234);
message.put(Field.NAMESPACE, "eventlog");
message.put(Field.ACTION, Action.UPSERT);
message.put(Field.TABLE_NAME, "events");
message.put(Field.KEY_NAMES, "event_id");
message.put(Field.SEQUENCE, System.currentTimeMillis());
message.put(Field.DATA, data);
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

// I can omit client id, namespace, table name, and key names
Map message = new HashMap();
message.put(Field.ACTION, Action.UPSERT);
message.put(Field.SEQUENCE, System.currentTimeMillis());
message.put(Field.DATA, data);
stitch.push(message);
```

Sending Messages
----------------

The Stitch client provides several methods for sending message,
including synchronous and asynchronous options. The synchronous
options are more straightforward, but the asynchronous options will
offer better performance if you are sending messages frequently.

### Synchronous Delivery

`push(Map message)` sends a single message and returns a
`StitchResponse`. It throws `StitchException` if stitch rejects the
record for some reason. There is typically no reason to inspect the
StitchResponse. If `push` doesn't throw, that means Stitch accepted
the message.

```java
try {
  stitch.push(message);
} catch (StitchException e) {
  // Handle error...
}
```

`push(List<Map> messages)` sends a list of messages at once. Note that
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

`put(Map message)`
