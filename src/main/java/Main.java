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
    ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor(); // Java 21+; warna newFixedThreadPool(50) use karo

    try (ServerSocket serverSocket = new ServerSocket(port)) {
      serverSocket.setReuseAddress(true);

      while (true) {                             // <-- ab yeh loop hamesha naye clients accept karega
        Socket clientSocket = serverSocket.accept();
        executor.submit(() -> handleClient(clientSocket));  // <-- har client alag thread/task me
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      executor.shutdown();
    }
  }

  private static void handleClient(Socket clientSocket) {
    try (clientSocket) {                          // try-with-resources -> khud close ho jaayega
      BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());

      while (true) {
        byte[] messageSizeBytes = in.readNBytes(4);
        if (messageSizeBytes.length < 4) {
          break; // client closed the connection
        }

        byte[] apiKey = in.readNBytes(2);
        byte[] apiVersion = in.readNBytes(2);
        int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();

        short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
        short errorCode = (apiVersionValue >= 0 && apiVersionValue <= 4) ? (short) 0 : (short) 35;

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(ByteBuffer.allocate(2).putShort(errorCode).array());
        body.write(2);
        body.write(ByteBuffer.allocate(2).putShort((short) 18).array());
        body.write(ByteBuffer.allocate(2).putShort((short) 0).array());
        body.write(ByteBuffer.allocate(2).putShort((short) 4).array());
        body.write(0);
        body.write(ByteBuffer.allocate(4).putInt(0).array());
        body.write(0);
        byte[] bodyBytes = body.toByteArray();

        int messageSize = 4 + bodyBytes.length;

        var out = clientSocket.getOutputStream();
        out.write(ByteBuffer.allocate(4).putInt(messageSize).array());
        out.write(ByteBuffer.allocate(4).putInt(correlationId).array());
        out.write(bodyBytes);
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}