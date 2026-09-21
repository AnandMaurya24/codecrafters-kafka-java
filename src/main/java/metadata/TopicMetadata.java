package metadata;

import java.util.List;
import java.util.UUID;

public record TopicMetadata(String name, UUID topicId, List<PartitionMetadata> partitions) {
}
