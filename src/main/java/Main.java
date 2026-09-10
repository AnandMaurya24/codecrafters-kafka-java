import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.err.println("Logs from your program will appear here!");

    // TODO: Uncomment the code below to pass the first stage
    // 
     ServerSocket serverSocket = null;
     Socket clientSocket = null;
     int port = 9092;
     try {
       serverSocket = new ServerSocket(port);
       // Since the tester restarts your program quite often, setting SO_REUSEADD
       // ensures that we don't run into 'Address already in use' errors
       serverSocket.setReuseAddress(true);
       // Wait for connection from client.
       clientSocket = serverSocket.accept();
       BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
       byte[] messageSizeBytes = in.readNBytes(4);
       int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();

       byte[] apiKey = in.readNBytes(2);
       byte[] apiVersion = in.readNBytes(2);
       int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();

       clientSocket.getOutputStream().write(messageSizeBytes);
       var res = ByteBuffer.allocate(4).putInt(correlationId).array();
       clientSocket.getOutputStream().write(res);
       clientSocket.getOutputStream().write(new byte[] {0, 35});
     } catch (IOException e) {
       System.out.println("IOException: " + e.getMessage());
     } finally {
       try {
         if (clientSocket != null) {
           clientSocket.close();
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       }
     }
  }
}
