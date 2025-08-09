package cc.mq.broker.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;

/**
 * @author nhsoft.lsd
 */
@Data
public class ConsumerGroupInfo {
    private String consumerGroup;
    private Map<String /*consumer id*/, ConsumerInfo> consumerInfoTables = new ConcurrentHashMap<>();

    @Data
    public static class ConsumerInfo {
        private String consumerId;

        private String consumerGroup;

        private Long lastLiveTimestamp;
    }

    public void add(ConsumerInfo consumerInfo) {
        consumerInfoTables.put(consumerInfo.getConsumerId(), consumerInfo);
    }

    public void remove(final String consumerId) {
        consumerInfoTables.remove(consumerId);
    }
}
