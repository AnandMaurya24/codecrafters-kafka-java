import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
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

       while (true) {
         byte[] messageSizeBytes = in.readNBytes(4);
         if (messageSizeBytes.length < 4) {
           break; // client closed the connection
         }

         int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt(); // size of THIS request
         byte[] apiKey = in.readNBytes(2);
         byte[] apiVersion = in.readNBytes(2);
         int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();

         // Skip the rest of the request (client_id, tagged fields, body) -
         // we only care about the header fields above for ApiVersions.
         int bytesReadSoFar = 2 + 2 + 4; // apiKey + apiVersion + correlationId
         in.skipNBytes(messageSize - bytesReadSoFar);

         short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
         short errorCode = (apiVersionValue >= 0 && apiVersionValue <= 4) ? (short) 0 : (short) 35;

         ByteArrayOutputStream body = new ByteArrayOutputStream();
         body.write(ByteBuffer.allocate(2).putShort(errorCode).array()); // error_code
         body.write(2);                                                  // api_keys: COMPACT_ARRAY length (1 entry -> N+1)
         body.write(ByteBuffer.allocate(2).putShort((short) 18).array()); // api_key: 18 (ApiVersions)
         body.write(ByteBuffer.allocate(2).putShort((short) 0).array());  // min_version
         body.write(ByteBuffer.allocate(2).putShort((short) 4).array());  // max_version
         body.write(0);                                                  // TAG_BUFFER for this api_keys entry
         body.write(ByteBuffer.allocate(4).putInt(0).array());            // throttle_time_ms
         body.write(0);                                                  // TAG_BUFFER for the response body
         byte[] bodyBytes = body.toByteArray();
         int responseSize = 4 + bodyBytes.length; // correlation_id + body

         var out = clientSocket.getOutputStream();
         out.write(ByteBuffer.allocate(4).putInt(responseSize).array());
         out.write(ByteBuffer.allocate(4).putInt(correlationId).array());
         out.write(bodyBytes);
       }
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
