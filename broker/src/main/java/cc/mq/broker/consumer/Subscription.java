package cc.mq.broker.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;

/**
 * @author nhsoft.lsd
 */
@Data
public class Subscription {

    private String topic;

    private Map<String /*consumer group*/, ConsumerGroupInfo> consumerGroupInfoTables = new ConcurrentHashMap<>();

    public void addConsumer(final String consumerGroup, final  String consumerId) {

        ConsumerGroupInfo.ConsumerInfo consumerInfo = new ConsumerGroupInfo.ConsumerInfo();
        consumerInfo.setConsumerId(consumerId);
        consumerInfo.setConsumerGroup(consumerGroup);
        consumerInfo.setLastLiveTimestamp(System.currentTimeMillis());

        ConsumerGroupInfo groupInfo = consumerGroupInfoTables.get(consumerInfo.getConsumerGroup());
        if (groupInfo == null) {
            groupInfo = new ConsumerGroupInfo();
            groupInfo.setConsumerGroup(consumerInfo.getConsumerGroup());
            groupInfo.add(consumerInfo);
            consumerGroupInfoTables.put(consumerInfo.getConsumerGroup(), groupInfo);
        } else {
            groupInfo.add(consumerInfo);
        }
    }

    public void removeConsumer(final String consumerGroup, final String consumerId) {
        ConsumerGroupInfo groupInfo = consumerGroupInfoTables.get(consumerGroup);
        if (groupInfo != null) {
            groupInfo.remove(consumerId);
        }
    }
}
