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

        byte[] apiKey = in.readNBytes(2);
        byte[] apiVersion = in.readNBytes(2);
        int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();

        // ab tak 8 bytes padh liye (apiKey + apiVersion + correlationId)
        // baaki bacha hua request body (client_id, software name/version, tag_buffer) discard karo
        // int remaining = requestMessageSize - 8;
        // if (remaining > 0) {
        //   in.readNBytes(remaining);
        // }
        byte[] client_id = in.readNBytes(2);
        byte[] contents = in.readNBytes(9);
        byte[] tag_buffer = in.readNBytes(1);
        int array_length = ByteBuffer.wrap(in.readNBytes(1)).getInt();
        int topic_name_length = ByteBuffer.wrap(in.readNBytes(1)).getInt();
        byte[] topic_name = in.readNBytes(topic_name_length - 1);
        int remaining = requestMessageSize - 8 - 2 - 9 - 1 - 1 - (topic_name_length - 1);
        if (remaining > 0) {
          in.readNBytes(remaining);
        } 

        short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
        short errorCode = (apiVersionValue >= 0 && apiVersionValue <= 4) ? (short) 0 : (short) 35;

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(0);                                   // tag_buffer
        body.write(ByteBuffer.allocate(4).putInt(0).array()); //throttle_time_ms
        body.write(2);  // topic_array
        body.write(ByteBuffer.allocate(2).putShort((short) 03).array()); //error_code ==> UNKNOWN_TOPIC_OR_PARTITION
        body.write(topic_name_length);                                    // name length:
        body.write(ByteBuffer.allocate(topic_name_length - 1).putShort((short) topic_name).array()); // topic name 
        body.write(ByteBuffer.allocate(16).putShort((short) 0).array()) //topic_id
        body.write(ByteBuffer.allocate(2).putShort((short) 0).array()); //is_internal
        body.write(1);                                                   // partitions array
        body.write(ByteBuffer.allocate(4).putShort((short) 0).array()); //topic_authorized_operations
        body.write(0);                                                   // tag_buffer
        body.write(ByteBuffer.allocate(2).putShort((short) -1).array());
        body.write(0);                                                   // tag_buffer
        // body.write(ByteBuffer.allocate(2).putShort(errorCode).array()); // error_code
        // body.write(3);                                                  // api_keys: COMPACT_ARRAY length (1 entry -> N+1)
        // body.write(ByteBuffer.allocate(2).putShort((short) 18).array()); // api_key: 18 (ApiVersions)
        // body.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
        // body.write(ByteBuffer.allocate(2).putShort((short) 4).array());  // max_version
        // body.write(0);                                                  // TAG_BUFFER for this api_keys entry
        // body.write(ByteBuffer.allocate(2).putShort((short) 75).array());
        // body.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
        // body.write(ByteBuffer.allocate(2).putShort((short) 0).array());
        // body.write(0);                                                  // TAG_BUFFER for this api_keys entry
        // body.write(ByteBuffer.allocate(4).putInt(0).array());            // throttle_time_ms
        // body.write(0);                                                  // TAG_BUFFER for the response body
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