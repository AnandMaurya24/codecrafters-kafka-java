package handler;

import dto.RequestHeader;
import enums.ApiKey;

import java.io.BufferedInputStream;
import java.io.IOException;

public interface ApiHandler {
    ApiKey apiKey();

    byte[] handle(RequestHeader header, BufferedInputStream in) throws IOException;
}
