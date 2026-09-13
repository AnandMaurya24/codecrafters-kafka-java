import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
  public static void main(String[] args) {
    System.err.println("Logs from your program will appear here!");

    int port = 9092;
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor(); // Java 21+; agar purana Java hai to newFixedThreadPool(50) use karo

    try (ServerSocket serverSocket = new ServerSocket(port)) {
      serverSocket.setReuseAddress(true);

      while (true) {                                   // hamesha naye clients accept karega
        Socket clientSocket = serverSocket.accept();
        executor.submit(() -> handleClient(clientSocket)); // har client alag thread/task me
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      executor.shutdown();
    }
  }

  private static void handleClient(Socket clientSocket) {
    try (clientSocket) {                               // try-with-resources -> khud close ho jaayega
      BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());

      while (true) {
        byte[] messageSizeBytes = in.readNBytes(4);
        if (messageSizeBytes.length < 4) {
          break; // client closed the connection
        }
        int requestMessageSize = ByteBuffer.wrap(messageSizeBytes).getInt();

        byte[] apiKeyBytes = in.readNBytes(2);
        byte[] apiVersionBytes = in.readNBytes(2);
        int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();

        short apiKeyValue = ByteBuffer.wrap(apiKeyBytes).getShort();
        short apiVersionValue = ByteBuffer.wrap(apiVersionBytes).getShort();

        // ---- request header (v1/v2): client_id (NULLABLE_STRING) + tag_buffer ----
        int clientIdLength = ByteBuffer.wrap(in.readNBytes(2)).getShort();
        byte[] clientId = in.readNBytes(clientIdLength);
        in.readNBytes(1); // request header tag_buffer

        ByteArrayOutputStream body = new ByteArrayOutputStream();

        if (apiKeyValue == 18) {
          // =========================================================
          // ApiVersions (api_key 18) request body:
          //   client_software_name (COMPACT_STRING) + client_software_version (COMPACT_STRING) + tag_buffer
          // =========================================================
          int nameLen = in.readNBytes(1)[0] & 0xFF;
          if (nameLen > 0) in.readNBytes(nameLen - 1); // client software name, discard
          int verLen = in.readNBytes(1)[0] & 0xFF;
          if (verLen > 0) in.readNBytes(verLen - 1);   // client software version, discard
          in.readNBytes(1);                            // tag_buffer

          short errorCode = (apiVersionValue >= 0 && apiVersionValue <= 4) ? (short) 0 : (short) 35;

          // Response header v0 for ApiVersions -> no tag_buffer here, just correlation_id (written later)
          body.write(ByteBuffer.allocate(2).putShort(errorCode).array()); // error_code
          body.write(3);                                                  // COMPACT_ARRAY length (2 entries -> 3)

          body.write(ByteBuffer.allocate(2).putShort((short) 18).array()); // api_key 18 (ApiVersions)
          body.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
          body.write(ByteBuffer.allocate(2).putShort((short) 4).array());  // max_version
          body.write(0);                                                  // TAG_BUFFER for entry 1

          body.write(ByteBuffer.allocate(2).putShort((short) 75).array()); // api_key 75 (DescribeTopicPartitions)
          body.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
          body.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // max_version
          body.write(0);                                                  // TAG_BUFFER for entry 2

          body.write(ByteBuffer.allocate(4).putInt(0).array());            // throttle_time_ms
          body.write(0);                                                  // TAG_BUFFER for the response body

        } else if (apiKeyValue == 75) {
          // =========================================================
          // DescribeTopicPartitions (api_key 75) request body:
          //   topics: COMPACT_ARRAY of { topic_name: COMPACT_STRING, tag_buffer }
          //   response_partition_limit: INT32
          //   cursor: nullable
          //   tag_buffer
          // =========================================================
          int arrayLength = in.readNBytes(1)[0] & 0xFF;      // topics array length (entries+1)
          int topicNameLength = in.readNBytes(1)[0] & 0xFF;  // compact string length (len+1)
          byte[] topicName = in.readNBytes(topicNameLength - 1);
          in.readNBytes(1); // tag_buffer after this topic entry

          int consumedSoFar = 8                      // apiKey+apiVersion+correlationId
              + 2 + clientIdLength                    // client_id length prefix + content
              + 1                                     // request header tag_buffer
              + 1                                     // topics array length byte
              + 1                                     // topic_name length byte
              + (topicNameLength - 1)                 // topic_name content
              + 1;                                    // topic entry tag_buffer
          int remaining = requestMessageSize - consumedSoFar;
          if (remaining > 0) {
            in.readNBytes(remaining); // response_partition_limit + cursor + body tag_buffer
          }

          // Response header v1 for DescribeTopicPartitions -> tag_buffer goes first in the body
          body.write(0);                                                   // response header tag_buffer
          body.write(ByteBuffer.allocate(4).putInt(0).array());             // throttle_time_ms
          body.write(2);                                                    // topics array length (1 entry -> 2)
          body.write(ByteBuffer.allocate(2).putShort((short) 3).array());   // error_code: UNKNOWN_TOPIC_OR_PARTITION
          body.write(topicNameLength);                                      // topic_name length (compact)
          body.write(topicName);                                            // topic_name
          body.write(new byte[16]);                                         // topic_id (all zeros)
          body.write(0);                                                    // is_internal (false)
          body.write(1);                                                    // partitions array (empty -> 1)
          body.write(ByteBuffer.allocate(4).putInt(0).array());              // topic_authorized_operations
          body.write(0);                                                     // TAG_BUFFER for this topic entry
          body.write(0xFF);                                                  // next_cursor (-1 / null)
          body.write(0);                                                     // TAG_BUFFER for the response body
        }

        byte[] bodyBytes = body.toByteArray();
        int responseMessageSize = 4 + bodyBytes.length; // correlation_id + body

        var out = clientSocket.getOutputStream();
        out.write(ByteBuffer.allocate(4).putInt(responseMessageSize).array());
        out.write(ByteBuffer.allocate(4).putInt(correlationId).array());
        out.write(bodyBytes);
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}