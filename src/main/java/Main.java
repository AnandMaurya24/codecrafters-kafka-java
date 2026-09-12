while (true) {
  byte[] messageSizeBytes = in.readNBytes(4);
  if (messageSizeBytes.length < 4) {
    break; // client closed the connection
  }
  int requestMessageSize = ByteBuffer.wrap(messageSizeBytes).getInt();

  byte[] apiKey = in.readNBytes(2);
  byte[] apiVersion = in.readNBytes(2);
  int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();

  // ab tak 8 bytes padhe (apiKey+apiVersion+correlationId)
  int remaining = requestMessageSize - 8;
  if (remaining > 0) {
    in.readNBytes(remaining); // client_id, software name/version, tag_buffer — discard kar do
  }

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

  int responseMessageSize = 4 + bodyBytes.length;

  var out = clientSocket.getOutputStream();
  out.write(ByteBuffer.allocate(4).putInt(responseMessageSize).array());
  out.write(ByteBuffer.allocate(4).putInt(correlationId).array());
  out.write(bodyBytes);
}