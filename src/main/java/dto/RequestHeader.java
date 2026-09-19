package dto;

public record RequestHeader(
        int requestMessageSize,
        short apiKey,
        short apiVersion,
        int correlationId,
        int clientIdLength,
        byte[] clientId
) {
}
