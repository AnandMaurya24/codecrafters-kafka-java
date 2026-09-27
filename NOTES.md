# Implementation Notes

Notes on how this broker is put together, and the Kafka wire-protocol concepts
picked up along the way while building it.

## Architecture

```
src/main/java/
├── Main.java                              — accept loop + handler registry wiring
├── dto/RequestHeader.java                 — record: parsed request header fields
├── enums/ApiKey.java                      — API_VERSIONS, DESCRIBE_TOPIC_PARTITIONS (key + min/max version)
├── handler/
│   ├── ApiHandler.java                    — contract every API implements
│   ├── ApiVersionsHandler.java
│   └── DescribeTopicPartitionsHandler.java
├── metadata/
│   ├── ClusterMetadataReader.java         — parses the __cluster_metadata log
│   ├── ClusterMetadata.java               — topic name -> TopicMetadata lookup
│   ├── TopicMetadata.java                 — record: name, topicId, partitions
│   └── PartitionMetadata.java             — record: index, leader, leaderEpoch, replicas, isr
└── util/PropertiesLoader.java             — reads server.properties (log.dirs)
```

This is a **Strategy + Template Method** design: `Main` doesn't know anything
about any specific Kafka API. It reads the generic request header, looks up a
handler by API key in a registry `Map<Short, ApiHandler>`, and delegates:

```java
ApiHandler handler = handlers.get(header.apiKey());
byte[] bodyBytes = handler != null ? handler.handle(header, in) : new byte[0];
```

Adding a new API (e.g. `Fetch`) means writing one new class that implements
`ApiHandler` and registering it in `Main.buildHandlerRegistry()` — no changes
to the socket loop, dispatch logic, or any other handler.

One deliberate trick in the registry:

```java
List<ApiHandler> handlers = new ArrayList<>();
handlers.add(new ApiVersionsHandler(handlers)); // sees this same live list, itself included
handlers.add(new DescribeTopicPartitionsHandler(clusterMetadata));
```

`ApiVersionsHandler` is handed the *same mutable list* before it's even fully
populated. That's safe because the list is only read later, inside
`handle()`, by which point every handler has been added — so `ApiVersions`
reports "here's everything this server supports" by reading the live
registry, instead of maintaining a second hardcoded list that could drift out
of sync.

## Kafka wire protocol concepts learned

### Compact encoding (`N+1`, not `N`)

Newer Kafka protocol versions encode array and string lengths as `N + 1`,
with `0` reserved to mean "null". Every length read/write in this codebase
has a `-1`/`+1` next to it because of this:

```java
int topicsArrayLength = (in.readNBytes(1)[0] & 0xFF) - 1; // entries+1 -> N
...
body.write(sortedTopicNames.size() + 1);                   // N -> entries+1
```

### TAG_BUFFER (tagged fields)

Most structures in the newer request/response versions end with a
`TAG_BUFFER` — a placeholder for optional/future fields. When empty (which is
all this project ever sends or expects), it's a single `0x00` byte. It shows
up after almost every entry: request headers, each topic entry, each
partition entry, and the response body itself.

### Varints, and zigzag encoding specifically

Two different varint flavors appear in the protocol:

- **Unsigned varint** — used for compact string/array lengths. Plain 7-bits-
  per-byte, high bit = "more bytes follow".
- **Signed varint (zigzag)** — used for record-batch framing fields
  (`length`, `timestampDelta`, `offsetDelta`, `keyLength`, ...). Zigzag maps
  signed integers to unsigned ones so small negative numbers (like `-1` for
  "null key") still encode as a single byte, instead of the top bits of a
  two's-complement int forcing a 5-byte varint:

  ```java
  private static int readUnsignedVarint(ByteBuffer buffer) {
      int value = 0, shift = 0, b;
      do {
          b = buffer.get() & 0xFF;
          value |= (b & 0x7F) << shift;
          shift += 7;
      } while ((b & 0x80) != 0);
      return value;
  }

  private static int readVarint(ByteBuffer buffer) {
      int raw = readUnsignedVarint(buffer);
      return (raw >>> 1) ^ -(raw & 1);   // zigzag decode
  }
  ```

  Zigzag maps `0,-1,1,-2,2,...` to `0,1,2,3,4,...` — so `-1` becomes `1`
  (fits in one byte) instead of `0xFFFFFFFF` (would need five).

### Self-describing frames, not length-prefixed blobs

The request body isn't "read N bytes, then parse them" — every field is
read in sequence, and *variable-length* fields (compact strings/arrays)
carry their own length inline. This means the parser has to track how many
bytes it has consumed to know how much of the request is left:

```java
int remaining = header.requestMessageSize() - consumedSoFar;
if (remaining > 0) {
    in.readNBytes(remaining); // response_partition_limit + cursor + body tag_buffer
}
```

Rather than decode every trailing field we don't care about, the handler
just computes what's left from the declared message size and skips it in
one read.

### Record batches: self-length-prefixed, skip what you don't need

The `__cluster_metadata` log file uses Kafka's **RecordBatch** format:
a batch header (with a `batchLength` field) wraps a sequence of records,
each of which *also* declares its own length up front:

```java
int length = readVarint(buffer);
int recordEnd = buffer.position() + length;

buffer.get();               // attributes
readVarint(buffer);         // timestamp_delta
readVarint(buffer);         // offset_delta
int keyLength = readVarint(buffer);
if (keyLength > 0) buffer.position(buffer.position() + keyLength);

int valueLength = readVarint(buffer);
if (valueLength > 0) {
    readMetadataValue(buffer, ...);   // only decodes record types we care about
}

buffer.position(recordEnd);  // jump to the next record regardless of what we parsed
```

This is the key idea that made the metadata parser simple: because every
batch and every record is **self-length-prefixed**, the reader never has to
fully understand a record to skip past it. Unknown record types (feature
level records, etc.) and unneeded trailing fields (partition epoch,
directories, headers) are all handled the same way — by jumping straight to
`recordEnd`/`batchEnd` — instead of writing a branch for every possible
field or record type.

### Joining records by ID, not by position

Inside `__cluster_metadata`, a `TopicRecord` (name → UUID) and its
`PartitionRecord`s (UUID → partition info) are independent records that can
appear in any order in the log. The reader builds two maps during the scan
and joins them once at the end:

```java
Map<String, TopicMetadata> topicsByName = new HashMap<>();
topicNamesById.forEach((topicId, name) -> topicsByName.put(name,
        new TopicMetadata(name, topicId, partitionsByTopicId.getOrDefault(topicId, List.of()))));
```

This mirrors how the real log works (a topic's partitions can be assigned
before or after the topic record itself is written) without requiring the
records to be read in any particular order.

### Sorting is a response-building concern, not a parsing concern

`DescribeTopicPartitions` must return topics sorted alphabetically,
regardless of the order the client asked for them in. Rather than complicate
parsing, the handler just parses into a list, then sorts a copy right
before writing the response:

```java
List<String> sortedTopicNames = new ArrayList<>(requestedTopicNames);
sortedTopicNames.sort(Comparator.naturalOrder());
```

Keeping "read what was asked" and "decide the order to answer in" as
separate steps made this a one-line addition instead of a rewrite.
