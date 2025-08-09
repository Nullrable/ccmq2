package cc.mq.broker.consumer;

import cc.mq.broker.meta.IndexedStore;
import jakarta.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * @author nhsoft.lsd
 */
@Component
public class MessageQueueStore {

    private Map<String, MessageQueue> messageQueues = new ConcurrentHashMap<>();

    @Resource
    private IndexedStore indexedStore;

    /**
     * 确认消息.
     *
     * @param topic   主题
     * @param queueId 队列ID
     * @return 当前消费位点. 下次消费时，客户端需要 +1， 目前不支持批量消费
     */
    public Long ack(String topic, Integer queueId) {
        String key = topic + "_" + queueId;
        MessageQueue messageQueue = messageQueues.get(key);
        if (messageQueue == null) {
            messageQueue = new MessageQueue(topic, queueId);
            messageQueues.put(key, messageQueue);
        }
        AtomicLong reqOffset = messageQueue.getOffset();
        Long curOffset = indexedStore.getTareIndex(topic, queueId);
        if (reqOffset.get() > curOffset) {
            return curOffset;
        }
        return messageQueue.ack();
    }
}
