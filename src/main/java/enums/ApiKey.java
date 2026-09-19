package enums;

public enum ApiKey {
    API_VERSIONS((short) 18, (short) 0, (short) 4),
    DESCRIBE_TOPIC_PARTITIONS((short) 75, (short) 0, (short) 0);

    private final short key;
    private final short minVersion;
    private final short maxVersion;

    ApiKey(short key, short minVersion, short maxVersion) {
        this.key = key;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public short getKey() {
        return key;
    }

    public short getMinVersion() {
        return minVersion;
    }

    public short getMaxVersion() {
        return maxVersion;
    }
}
