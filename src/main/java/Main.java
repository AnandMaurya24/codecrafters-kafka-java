import dto.RequestHeader;
import handler.ApiHandler;
import handler.ApiVersionsHandler;
import handler.DescribeTopicPartitionsHandler;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Main {
  public static void main(String[] args) {
    System.err.println("Logs from your program will appear here!");

    int port = 9092;
    Map<Short, ApiHandler> handlers = buildHandlerRegistry();
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor(); // Java 21+; agar purana Java hai to newFixedThreadPool(50) use karo

    try (ServerSocket serverSocket = new ServerSocket(port)) {
      serverSocket.setReuseAddress(true);

      while (true) {                                   // hamesha naye clients accept karega
        Socket clientSocket = serverSocket.accept();
        executor.submit(() -> handleClient(clientSocket, handlers)); // har client alag thread/task me
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      executor.shutdown();
    }
  }

  private static Map<Short, ApiHandler> buildHandlerRegistry() {
    List<ApiHandler> handlers = new ArrayList<>();
    handlers.add(new ApiVersionsHandler(handlers)); // sees this same live list, itself included
    handlers.add(new DescribeTopicPartitionsHandler());
    return handlers.stream().collect(Collectors.toMap(h -> h.apiKey().getKey(), h -> h));
  }

  private static void handleClient(Socket clientSocket, Map<Short, ApiHandler> handlers) {
    try (clientSocket) {                               // try-with-resources -> khud close ho jaayega
      BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());

      while (true) {
        RequestHeader header = readRequestHeader(in);
        if (header == null) {
          break; // client closed the connection
        }

        ApiHandler handler = handlers.get(header.apiKey());
        byte[] bodyBytes = handler != null ? handler.handle(header, in) : new byte[0];
        int responseMessageSize = 4 + bodyBytes.length; // correlation_id + body

        var out = clientSocket.getOutputStream();
        out.write(ByteBuffer.allocate(4).putInt(responseMessageSize).array());
        out.write(ByteBuffer.allocate(4).putInt(header.correlationId()).array());
        out.write(bodyBytes);
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  private static RequestHeader readRequestHeader(BufferedInputStream in) throws IOException {
    byte[] messageSizeBytes = in.readNBytes(4);
    if (messageSizeBytes.length < 4) {
      return null;
    }
    int requestMessageSize = ByteBuffer.wrap(messageSizeBytes).getInt();

    short apiKey = ByteBuffer.wrap(in.readNBytes(2)).getShort();
    short apiVersion = ByteBuffer.wrap(in.readNBytes(2)).getShort();
    int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();

    // request header (v1/v2): client_id (NULLABLE_STRING) + tag_buffer
    int clientIdLength = ByteBuffer.wrap(in.readNBytes(2)).getShort();
    byte[] clientId = in.readNBytes(clientIdLength);
    in.readNBytes(1); // request header tag_buffer

    return new RequestHeader(requestMessageSize, apiKey, apiVersion, correlationId, clientIdLength, clientId);
  }
}
