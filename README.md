Stitch client for Java.

Quick Start
===========

This will get you started sending records to Stitch. You'll go through
the following steps:

1. Build a StitchClient
2. Build messages
3. Push messages to stitch

Building a Client
-----------------

Use StitchClientBuilder to build a stitch client. You'll need to set
your client id, authentication token, and namespace. You should have
gotten these when you set up the integration at
http://stitchdata.com. You should close the client when you're done
with it to ensure that all messages are delivered, so we recommend
opening it in a try-with-resources statement.

```java
try (StitchClient stitch = new StitchClientBuilder()
     .withClientId(yourClientId)
     .withToken(yourToken)
     .withNamespace(yourNamespace)
     .build())
{
  // ...
}
```

Building a Message
------------------

You can build a Stitch message by creating a new instance of
StitchMessage and then calling methods on it to set the properties of
the message. For example:

```java
StitchMessage message = StitchMessage.newUpsert()
    .withTableName("my_table")
    .withKeyNames("id")
    .withSequence(System.currentTimeMillis())
    .withData(data);
```

* Table name is the name of the table you want to load into
* Key names is the list of primary key columns for that table
* Sequence is any arbitrary increasing number used to determine order of updates
* Data is the payload

Data must be a map that conforms to the following rules:

* All keys are strings
* All values are one of:
  * Number (Long, Integer, Short, Byte, Double, Float, BigInteger, BigDecimal)
  * String
  * Boolean
  * Date
  * Map (with string keys and values that conform to these rules)
  * Lists (of objects that conform to these rules
* It must have a non-null value for each of the keys you specified as "key names"

Sending Messages
----------------

You send a message to Stitch by calling the `push` method on your
`StitchClient` instance, and passing in a `StitchMessage`.

```java
stitch.push(message);
```

Please see SimpleExample.java for a full working example.

Advanced Topics
===============

Setting message defaults on the client
--------------------------------------

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

Tuning Buffer Parameters
------------------------

By default `stitchClient.push()` will accumulate messages locally in a
batch, and then deliver the batch when one of the following conditions
is met:

* The batch has 4 Mb of data
* The batch has 10,000 records
* A minute has passed since the last batch was sent.

If you want to send data more frequently, you can lower the buffer
capacity or the time limit.

```java
StitchClient stitch = new StitchClientBuilder()
  .withClientId(yourClientId)
  .withToken(yourToken)
  .withNamespace(yourNamespace)

  // Flush at 1Mb
  .withBatchSize(1000000)

  // Flush after 1 minute
  .withBatchDelayMillis(10000)
  .build();
```

Setting the batch size to 0 bytes will effectively turn off batching
and force `push` to send a batch of one record with every call. This
is not generally recommended, as batching will give better
performance, but can be useful for low-volume streams or for
debugging.

There is no value in setting a buffer capacity higher than 4 Mb, since
that is the maximum message size Stitch will accept. If you set it to
a value higher than that, you will use more memory, but StitchClient
will deliver the messages in batches no larger than 4 Mb anyway.

Asynchronous Usage
------------------

StitchClient is *not* thread-safe. Calling any of methods concurrently
can result in lost or corrupt data. If your application has multiple
threads producing data, we recommend using a separate client for each
thread.
