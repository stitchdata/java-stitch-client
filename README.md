## Building a Client

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
