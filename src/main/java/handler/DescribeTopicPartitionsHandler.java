package handler;

import dto.RequestHeader;
import enums.ApiKey;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DescribeTopicPartitionsHandler implements ApiHandler {

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
        byte[] topicName = in.readNBytes(topicNameLength - 1);
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

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(0);                                                   // response header tag_buffer
        body.write(ByteBuffer.allocate(4).putInt(0).array());             // throttle_time_ms
        body.write(2);                                                    // topics array length (1 entry -> 2)
        body.write(ByteBuffer.allocate(2).putShort((short) 3).array());   // error_code: UNKNOWN_TOPIC_OR_PARTITION
        body.write(topicNameLength);                                      // topic_name length (compact)
        body.write(topicName);                                            // topic_name
        body.write(new byte[16]);                                         // topic_id (all zeros)
        body.write(0);                                                    // is_internal (false)
        body.write(1);                                                    // partitions array (empty -> 1)
        body.write(ByteBuffer.allocate(4).putInt(0).array());             // topic_authorized_operations
        body.write(0);                                                    // TAG_BUFFER for this topic entry
        body.write(0xFF);                                                 // next_cursor (-1 / null)
        body.write(0);                                                    // TAG_BUFFER for the response body
        return body.toByteArray();
    }
}
