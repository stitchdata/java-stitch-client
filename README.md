Quick Start
===========

* Have your Stitch client id, access token, and namespace handy
* Build an instance of StitchClient
* Build messages
* Push messages to stitch
* Close the client

Stitch Client ID, Access Token, and Namespace
---------------------------------------------

TODO: How to get them?

Building a Client
-----------------

Use StitchClientBuilder to build a stitch client. You'll need to set
your client id, authentication token, and namespace.

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

Sending Messages
----------------

You send a message to Stitch by calling the `push` method on your
`StitchClient` instance, and passing in a `StitchMessage`.

```java
try {
    stitch.push(message);
}
catch (StitchException e) {
    System.err.println("Error sending message to stitch: " + e.getMessage());
}
```

Close the client
----------------

try {
    stitch.close();
} catch (StitchException e) {
   System.err.println("Error closing stitch client: " + e.getMessage());
}

Guide
=====

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

Sending Records in Batches
--------------------------

By default, every call to `StitchClient.push(StitchMessage)` will
result in an HTTP request to Stitch. For high data volumes, you will
get much better performance by allowing StitchClient to buffer your
messages in memory and deliver them in batches. You can turn on
buffering by calling `withBufferCapacity` on your
`StitchClientBuilder`.

```java
StitchClient stitch = new StitchClientBuilder()
  .withClientId(yourClientId)
  .withToken(yourToken)
  .withNamespace(yourNamespace)
  .withBufferCapacity(1000000)
  .withBufferTimeLimit(
  .build();
```

There is no value in setting a buffer capacity higher than 4Mb, since
that is the maximum message size Stitch will accept.
