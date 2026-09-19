package handler;

import dto.RequestHeader;
import enums.ApiKey;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ApiVersionsHandler implements ApiHandler {
    private final List<ApiHandler> registeredHandlers;

    /**
     * @param registeredHandlers the live registry from Main; read at request time, so it can
     *                           include this handler itself as long as it's populated by then.
     */
    public ApiVersionsHandler(List<ApiHandler> registeredHandlers) {
        this.registeredHandlers = registeredHandlers;
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.API_VERSIONS;
    }

    @Override
    public byte[] handle(RequestHeader header, BufferedInputStream in) throws IOException {
        // ApiVersions (api_key 18) request body:
        //   client_software_name (COMPACT_STRING) + client_software_version (COMPACT_STRING) + tag_buffer
        int nameLen = in.readNBytes(1)[0] & 0xFF;
        if (nameLen > 0) in.readNBytes(nameLen - 1); // client software name, discard
        int verLen = in.readNBytes(1)[0] & 0xFF;
        if (verLen > 0) in.readNBytes(verLen - 1);   // client software version, discard
        in.readNBytes(1);                            // tag_buffer

        ApiKey self = apiKey();
        boolean supported = header.apiVersion() >= self.getMinVersion() && header.apiVersion() <= self.getMaxVersion();
        short errorCode = supported ? (short) 0 : (short) 35;

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(ByteBuffer.allocate(2).putShort(errorCode).array()); // error_code
        body.write(registeredHandlers.size() + 1);                     // COMPACT_ARRAY length (N entries -> N+1)

        for (ApiHandler handler : registeredHandlers) {
            ApiKey key = handler.apiKey();
            body.write(ByteBuffer.allocate(2).putShort(key.getKey()).array());
            body.write(ByteBuffer.allocate(2).putShort(key.getMinVersion()).array());
            body.write(ByteBuffer.allocate(2).putShort(key.getMaxVersion()).array());
            body.write(0); // TAG_BUFFER for this entry
        }

        body.write(ByteBuffer.allocate(4).putInt(0).array()); // throttle_time_ms
        body.write(0);                                        // TAG_BUFFER for the response body
        return body.toByteArray();
    }
}
