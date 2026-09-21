package metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Parses the __cluster_metadata log (Kafka record-batch format) to recover topic
 * names/UUIDs and their partitions. Only TopicRecord and PartitionRecord payloads are
 * decoded; every other record type, and every trailing field we don't need (partition
 * epoch, directories, tagged fields, headers, ...), is skipped by jumping straight to
 * the next record/batch using the length each one already carries in its own framing.
 */
public class ClusterMetadataReader {
    private static final byte TOPIC_RECORD_TYPE = 2;
    private static final byte PARTITION_RECORD_TYPE = 3;

    public static ClusterMetadata read(Path logFile) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(logFile));

        Map<UUID, String> topicNamesById = new HashMap<>();
        Map<UUID, List<PartitionMetadata>> partitionsByTopicId = new HashMap<>();

        while (buffer.remaining() > 0) {
            readBatch(buffer, topicNamesById, partitionsByTopicId);
        }

        Map<String, TopicMetadata> topicsByName = new HashMap<>();
        topicNamesById.forEach((topicId, name) -> topicsByName.put(name,
                new TopicMetadata(name, topicId, partitionsByTopicId.getOrDefault(topicId, List.of()))));

        return new ClusterMetadata(topicsByName);
    }

    public static ClusterMetadata readSafely(Path logFile) {
        try {
            return read(logFile);
        } catch (IOException e) {
            System.out.println("Could not read cluster metadata log " + logFile + ": " + e.getMessage());
            return new ClusterMetadata(Map.of());
        }
    }

    private static void readBatch(ByteBuffer buffer, Map<UUID, String> topicNamesById,
                                   Map<UUID, List<PartitionMetadata>> partitionsByTopicId) {
        buffer.getLong();                      // base_offset
        int batchLength = buffer.getInt();
        int batchEnd = buffer.position() + batchLength;

        buffer.getInt();                       // partition_leader_epoch
        buffer.get();                          // magic
        buffer.getInt();                       // crc
        buffer.getShort();                     // attributes
        buffer.getInt();                       // last_offset_delta
        buffer.getLong();                      // base_timestamp
        buffer.getLong();                      // max_timestamp
        buffer.getLong();                      // producer_id
        buffer.getShort();                     // producer_epoch
        buffer.getInt();                       // base_sequence
        int recordsCount = buffer.getInt();

        for (int i = 0; i < recordsCount; i++) {
            readRecord(buffer, topicNamesById, partitionsByTopicId);
        }

        buffer.position(batchEnd);
    }

    private static void readRecord(ByteBuffer buffer, Map<UUID, String> topicNamesById,
                                    Map<UUID, List<PartitionMetadata>> partitionsByTopicId) {
        int length = readVarint(buffer);
        int recordEnd = buffer.position() + length;

        buffer.get();                          // attributes
        readVarint(buffer);                    // timestamp_delta
        readVarint(buffer);                    // offset_delta

        int keyLength = readVarint(buffer);
        if (keyLength > 0) {
            buffer.position(buffer.position() + keyLength);
        }

        int valueLength = readVarint(buffer);
        if (valueLength > 0) {
            readMetadataValue(buffer, topicNamesById, partitionsByTopicId);
        }

        buffer.position(recordEnd);
    }

    private static void readMetadataValue(ByteBuffer buffer, Map<UUID, String> topicNamesById,
                                           Map<UUID, List<PartitionMetadata>> partitionsByTopicId) {
        buffer.get();                          // frame_version
        byte type = buffer.get();
        buffer.get();                          // record version

        if (type == TOPIC_RECORD_TYPE) {
            String name = readCompactString(buffer);
            UUID topicId = readUuid(buffer);
            topicNamesById.put(topicId, name);
        } else if (type == PARTITION_RECORD_TYPE) {
            int partitionId = buffer.getInt();
            UUID topicId = readUuid(buffer);
            int[] replicas = readCompactInt32Array(buffer);
            int[] isr = readCompactInt32Array(buffer);
            readCompactInt32Array(buffer);     // removing_replicas, not needed for the response
            readCompactInt32Array(buffer);     // adding_replicas, not needed for the response
            int leader = buffer.getInt();
            int leaderEpoch = buffer.getInt();

            partitionsByTopicId
                    .computeIfAbsent(topicId, id -> new ArrayList<>())
                    .add(new PartitionMetadata(partitionId, leader, leaderEpoch, replicas, isr));
        }
        // other record types (feature level, etc.) aren't needed to answer DescribeTopicPartitions
    }

    private static UUID readUuid(ByteBuffer buffer) {
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    private static String readCompactString(ByteBuffer buffer) {
        int length = readUnsignedVarint(buffer) - 1;
        byte[] bytes = new byte[Math.max(length, 0)];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int[] readCompactInt32Array(ByteBuffer buffer) {
        int length = readUnsignedVarint(buffer) - 1;
        int[] values = new int[Math.max(length, 0)];
        for (int i = 0; i < values.length; i++) {
            values[i] = buffer.getInt();
        }
        return values;
    }

    private static int readUnsignedVarint(ByteBuffer buffer) {
        int value = 0;
        int shift = 0;
        int b;
        do {
            b = buffer.get() & 0xFF;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    private static int readVarint(ByteBuffer buffer) {
        int raw = readUnsignedVarint(buffer);
        return (raw >>> 1) ^ -(raw & 1);
    }
}
