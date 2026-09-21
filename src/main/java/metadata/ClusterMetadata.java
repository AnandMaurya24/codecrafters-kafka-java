package metadata;

import java.util.Map;
import java.util.Optional;

public class ClusterMetadata {
    private final Map<String, TopicMetadata> topicsByName;

    public ClusterMetadata(Map<String, TopicMetadata> topicsByName) {
        this.topicsByName = topicsByName;
    }

    public Optional<TopicMetadata> findByName(String name) {
        return Optional.ofNullable(topicsByName.get(name));
    }
}
