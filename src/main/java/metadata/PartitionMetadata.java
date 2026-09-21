package metadata;

public record PartitionMetadata(int partitionIndex, int leaderId, int leaderEpoch, int[] replicaNodes, int[] isrNodes) {
}
