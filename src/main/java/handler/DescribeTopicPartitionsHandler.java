package handler;

import dto.RequestHeader;
import enums.ApiKey;
import metadata.ClusterMetadata;
import metadata.PartitionMetadata;
import metadata.TopicMetadata;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

public class DescribeTopicPartitionsHandler implements ApiHandler {
    private final ClusterMetadata clusterMetadata;

    public DescribeTopicPartitionsHandler(ClusterMetadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.DESCRIBE_TOPIC_PARTITIONS;
    }

    @Override
    public byte[] handle(RequestHeader header, BufferedInputStream in) throws IOException {
        // DescribeTopicPartitions (api_key 75) request body:
        //   topics: COMPACT_ARRAY of { topic_name: COMPACT_STRING, tag_buffer }
        //   response_partition_limit: INT32
        //   cursor: nullable
        //   tag_buffer
        in.readNBytes(1);                                   // topics array length (entries+1), unused
        int topicNameLength = in.readNBytes(1)[0] & 0xFF;    // compact string length (len+1)
        byte[] topicNameBytes = in.readNBytes(topicNameLength - 1);
        in.readNBytes(1); // tag_buffer after this topic entry

        int consumedSoFar = 8                          // apiKey+apiVersion+correlationId
                + 2 + header.clientIdLength()           // client_id length prefix + content
                + 1                                     // request header tag_buffer
                + 1                                     // topics array length byte
                + 1                                     // topic_name length byte
                + (topicNameLength - 1)                 // topic_name content
                + 1;                                    // topic entry tag_buffer
        int remaining = header.requestMessageSize() - consumedSoFar;
        if (remaining > 0) {
            in.readNBytes(remaining); // response_partition_limit + cursor + body tag_buffer
        }

        String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
        Optional<TopicMetadata> topic = clusterMetadata.findByName(topicName);

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(0);                                             // response header tag_buffer
        body.write(ByteBuffer.allocate(4).putInt(0).array());       // throttle_time_ms
        body.write(2);                                              // topics array length (1 entry -> 2)

        if (topic.isPresent()) {
            writeKnownTopic(body, topicNameLength, topicNameBytes, topic.get());
        } else {
            writeUnknownTopic(body, topicNameLength, topicNameBytes);
        }

        body.write(0xFF); // next_cursor (-1 / null)
        body.write(0);    // TAG_BUFFER for the response body
        return body.toByteArray();
    }

    private void writeUnknownTopic(ByteArrayOutputStream body, int topicNameLength, byte[] topicNameBytes) throws IOException {
        body.write(ByteBuffer.allocate(2).putShort((short) 3).array()); // error_code: UNKNOWN_TOPIC_OR_PARTITION
        body.write(topicNameLength);                                     // topic_name length (compact)
        body.write(topicNameBytes);                                      // topic_name
        body.write(new byte[16]);                                        // topic_id (all zeros)
        body.write(0);                                                   // is_internal (false)
        body.write(1);                                                   // partitions array (empty -> 1)
        body.write(ByteBuffer.allocate(4).putInt(0).array());            // topic_authorized_operations
        body.write(0);                                                   // TAG_BUFFER for this topic entry
    }

    private void writeKnownTopic(ByteArrayOutputStream body, int topicNameLength, byte[] topicNameBytes,
                                  TopicMetadata topic) throws IOException {
        body.write(ByteBuffer.allocate(2).putShort((short) 0).array()); // error_code: none
        body.write(topicNameLength);                                     // topic_name length (compact)
        body.write(topicNameBytes);                                      // topic_name
        body.write(uuidToBytes(topic.topicId()));                        // topic_id (actual UUID from metadata)
        body.write(0);                                                   // is_internal (false)

        body.write(topic.partitions().size() + 1);                       // partitions array length (compact)
        for (PartitionMetadata partition : topic.partitions()) {
            body.write(ByteBuffer.allocate(2).putShort((short) 0).array());               // partition error_code: none
            body.write(ByteBuffer.allocate(4).putInt(partition.partitionIndex()).array()); // partition_index
            body.write(ByteBuffer.allocate(4).putInt(partition.leaderId()).array());       // leader_id
            body.write(ByteBuffer.allocate(4).putInt(partition.leaderEpoch()).array());    // leader_epoch
            writeCompactInt32Array(body, partition.replicaNodes());                        // replica_nodes
            writeCompactInt32Array(body, partition.isrNodes());                            // isr_nodes
            body.write(1); // eligible_leader_replicas: empty
            body.write(1); // last_known_elr: empty
            body.write(1); // offline_replicas: empty
            body.write(0); // TAG_BUFFER for this partition entry
        }

        body.write(ByteBuffer.allocate(4).putInt(0).array()); // topic_authorized_operations
        body.write(0);                                        // TAG_BUFFER for this topic entry
    }

    private static void writeCompactInt32Array(ByteArrayOutputStream body, int[] values) throws IOException {
        body.write(values.length + 1);
        for (int value : values) {
            body.write(ByteBuffer.allocate(4).putInt(value).array());
        }
    }

    private static byte[] uuidToBytes(UUID uuid) {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
